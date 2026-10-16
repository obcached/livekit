// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sfu_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/mediatransportutil/pkg/codec"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/livekit/livekit-server/pkg/testutils/vnettest"
)

const (
	// the rig sets the sequencer RTT to 10 ms: dedupe window min(100 ms, 2*10 ms) = 20 ms
	stepRigRTT           = 10
	stepRigOutsideWindow = 30 * time.Millisecond
)

type stepRigOpts struct {
	rtxCurb  bool
	keyFrame bool
	// maxSpatial 0 locks the forwarder at spatial 0, so the key frame requester stays quiet
	maxSpatial int32
	layer      buffer.VideoLayer
}

// stepRig is a bound VP8 simulcast DownTrack that commits congestion allocations
// (packet writer from the Lane K NACK rig).
type stepRig struct {
	t     *testing.T
	dh    *downTrackHarness
	dt    *DownTrack
	pubSN uint16
	pid   uint16
	ts    uint32
	tl0   uint8
}

func newStepRig(t *testing.T, o stepRigOpts) *stepRig {
	h := vnettest.NewHosts(t)
	factory := buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	p := pacer.NewPassThrough(logger.GetLogger(), newNullBWE())
	t.Cleanup(p.Stop)

	dh := newBoundDownTrack(t, h, factory, vp8CodecParams, p, vnettest.MediaEngineConfig{Video: true})
	dt := dh.dt
	// the harness reports MODE_UNUSED: re-run DetermineCodec in simulcast mode
	dh.receiver.VideoLayerModeReturns(livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM)
	dt.ReceiverRestart(dh.receiver)
	dt.SetRTXCurbOnTemporalDownStepForTest(o.rtxCurb)
	dt.SetKeyFrameOnTemporalDownStepForTest(o.keyFrame)
	dt.SetNACKRTTForTest(stepRigRTT)
	dt.SetMaxSpatialLayer(o.maxSpatial)
	dt.SetMaxTemporalLayer(2)
	dt.UpTrackMaxPublishedLayerChange(o.maxSpatial)
	dt.UpTrackMaxTemporalLayerSeenChange(2)
	var brs Bitrates
	for s := range brs {
		brs[s][0], brs[s][1], brs[s][2] = 100_000, 200_000, 400_000
	}
	dh.receiver.GetLayeredBitrateReturns([]int32{0, 1}, brs)
	dt.ForceForwardLayerForTest(o.layer)

	return &stepRig{t: t, dh: dh, dt: dt, pubSN: 1000, pid: 100, ts: 90000, tl0: 10}
}

// write offers one VP8 packet and reports whether the DownTrack forwarded it.
func (r *stepRig) write(spatial int32, tid uint8, key bool) (*buffer.ExtPacket, bool) {
	if tid == 0 {
		r.tl0++
	}
	params := &testutils.TestExtPacketParams{
		SequenceNumber: r.pubSN,
		Timestamp:      r.ts,
		SSRC:           0x12345678,
		PayloadSize:    20,
		Marker:         true,
		ArrivalTime:    time.Now(),
		VideoLayer:     buffer.VideoLayer{Spatial: spatial, Temporal: int32(tid)},
	}
	vp8 := &codec.VP8{
		FirstByte:  25,
		I:          true,
		M:          true,
		PictureID:  r.pid,
		L:          true,
		TL0PICIDX:  r.tl0,
		T:          true,
		TID:        tid,
		Y:          true,
		K:          true,
		KEYIDX:     23,
		HeaderSize: 6,
		IsKeyFrame: key,
	}
	extPkt, err := testutils.GetTestExtPacketVP8(params, vp8)
	require.NoError(r.t, err)
	r.pubSN++
	r.pid++
	r.ts += 3000
	return extPkt, r.dt.WriteRTP(extPkt, spatial) > 0
}

// writeForwarded writes a spatial 0 packet that must be forwarded.
func (r *stepRig) writeForwarded(tid uint8, key bool) {
	extPkt, forwarded := r.write(0, tid, key)
	require.True(r.t, forwarded, "precondition: packet sn %d (tid %d) must be forwarded", extPkt.ExtSequenceNumber, tid)
}

// subSNs waits for n forwarded packets at the subscriber and returns their (subscriber-side) sequence numbers.
func (r *stepRig) subSNs(n int) []uint16 {
	require.Eventually(r.t, func() bool {
		return r.dh.capture.count() >= n
	}, 10*time.Second, 10*time.Millisecond, "expected %d forwarded packets", n)
	sns := make([]uint16, 0, n)
	for _, pkt := range r.dh.capture.all() {
		sns = append(sns, pkt.SequenceNumber)
	}
	require.Len(r.t, sns, n)
	return sns
}

// commit runs one cooperative allocation of the given layer at the given capacity.
func (r *stepRig) commit(capacity int64, layer buffer.VideoLayer) VideoAllocation {
	r.dt.ProvisionalAllocatePrepare()
	ok, _ := r.dt.ProvisionalAllocate(capacity, layer, false, false)
	require.True(r.t, ok)
	return r.dt.ProvisionalAllocateCommit()
}

// stepDown commits a deficient temporal-only down-step from the current layer to target.
func (r *stepRig) stepDown(target buffer.VideoLayer) VideoAllocation {
	locked, _ := r.dt.ForwarderCheckSyncForTest()
	require.True(r.t, locked, "precondition: forwarder locked before the step, the key frame requester stays quiet")
	alloc := r.commit(250_000, target)
	require.True(r.t, alloc.IsDeficient)
	require.True(r.t, alloc.TemporalDownStep)
	require.Equal(r.t, target, alloc.TargetLayer)
	return alloc
}

// nack runs the retransmit path for one subscriber sequence number and returns the number of
// packets it tried to retransmit (ReadRTP calls; the fake receiver answers io.EOF) and the
// number of NACKed packets the curb skipped. It waits out the sequencer dedupe window first
// (a fresh sequencer also ignores NACKs within that window of its start).
func (r *stepRig) nack(sn uint16) (rtx int, curbed int) {
	time.Sleep(stepRigOutsideWindow)
	rtxBefore := r.dh.receiver.ReadRTPCallCount()
	curbedBefore := r.dt.RTXCurbedForTest()
	r.dt.RetransmitPacketsForTest([]uint16{sn})
	return r.dh.receiver.ReadRTPCallCount() - rtxBefore, int(r.dt.RTXCurbedForTest() - curbedBefore)
}

var (
	s0t2 = buffer.VideoLayer{Spatial: 0, Temporal: 2}
	s0t1 = buffer.VideoLayer{Spatial: 0, Temporal: 1}
	s0t0 = buffer.VideoLayer{Spatial: 0, Temporal: 0}
)

// preStep forwards a key frame and TL1/TL2/TL0 packets at s0t2 and returns their subscriber sequence numbers.
func (r *stepRig) preStep() []uint16 {
	r.writeForwarded(0, true)
	r.writeForwarded(1, false)
	r.writeForwarded(2, false)
	r.writeForwarded(0, false)
	return r.subSNs(4)
}

func TestDownTrackRTXCurbOnTemporalDownStep(t *testing.T) {
	t.Run("pre-step NACK is curbed, post-step NACK is repaired", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, layer: s0t2})
		sns := r.preStep()
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: no boundary before the step")

		r.stepDown(s0t1)
		require.EqualValues(t, sns[3], uint16(r.dt.RTXCurbBoundaryForTest()), "boundary = last forwarded subscriber sequence number")

		rtx, curbed := r.nack(sns[1])
		require.Zero(t, rtx, "pre-step packet must not be retransmitted")
		require.Equal(t, 1, curbed)

		rtx, curbed = r.nack(sns[3])
		require.Zero(t, rtx, "the boundary packet itself is pre-step")
		require.Equal(t, 1, curbed)

		r.writeForwarded(0, false)
		sns = r.subSNs(5)
		rtx, curbed = r.nack(sns[4])
		require.Equal(t, 1, rtx, "post-step packet keeps RTX")
		require.Zero(t, curbed)
	})

	t.Run("flag off: no boundary, pre-step NACK is repaired", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: false, layer: s0t2})
		sns := r.preStep()

		r.stepDown(s0t1)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest())

		rtx, curbed := r.nack(sns[1])
		require.Equal(t, 1, rtx)
		require.Zero(t, curbed)
	})

	t.Run("a forwarded key frame moves the boundary to just below it", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)
		require.EqualValues(t, sns[3], uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: boundary at the step head")

		rtx, curbed := r.nack(sns[1])
		require.Zero(t, rtx, "precondition: curb active")
		require.Equal(t, 1, curbed)

		r.writeForwarded(1, false) // post-step packet before the key frame
		r.writeForwarded(0, true)
		sns = r.subSNs(6)
		kf := sns[5]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "boundary = key frame sn - 1, not cleared")
		require.Zero(t, r.dt.RTXCurbedForTest(), "the move logs and resets the curbed count")

		rtx, curbed = r.nack(sns[1])
		require.Zero(t, rtx, "the pre-step backlog stays curbed after the key frame")
		require.Equal(t, 1, curbed)

		rtx, curbed = r.nack(sns[4])
		require.Zero(t, rtx, "packets older than the key frame are curbed")
		require.Equal(t, 1, curbed)

		rtx, curbed = r.nack(kf)
		require.Equal(t, 1, rtx, "the key frame itself is repaired")
		require.Zero(t, curbed)
	})

	t.Run("double step: the re-armed boundary never passes the key frame", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)

		// the selector reaches the target, K1 (answer to the first step) is forwarded,
		// then T1 traffic continues until the second step
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(1, false)
		r.writeForwarded(0, true)
		r.writeForwarded(1, false)
		r.writeForwarded(0, false)
		sns := r.subSNs(8)
		kf := sns[5]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		r.stepDown(s0t0)
		head := sns[7]
		boundary := uint16(r.dt.RTXCurbBoundaryForTest())
		require.NotEqual(t, head, boundary, "the second step must not arm at the head when a key frame was forwarded")
		require.EqualValues(t, kf-1, boundary, "second step boundary = min(head, kf-1)")

		for _, sn := range sns[5:8] {
			rtx, curbed := r.nack(sn)
			require.Equal(t, 1, rtx, "K1 and the packets after it keep RTX (sn %d)", sn)
			require.Zero(t, curbed)
		}

		rtx, curbed := r.nack(sns[4])
		require.Zero(t, rtx, "packets older than the key frame stay curbed")
		require.Equal(t, 1, curbed)
		rtx, curbed = r.nack(sns[1])
		require.Zero(t, rtx, "the first step's backlog stays curbed")
		require.Equal(t, 1, curbed)
	})

	t.Run("double step: a key frame right after the step head is still kept repairable", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)
		require.EqualValues(t, sns[3], uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: boundary at the step head")

		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(0, true) // K1 is the first packet after the boundary
		r.writeForwarded(1, false)
		sns = r.subSNs(6)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: boundary already just below K1")

		r.stepDown(s0t0)
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "second step boundary = kf-1, not the head")

		rtx, curbed := r.nack(kf)
		require.Equal(t, 1, rtx, "K1 keeps RTX after the second step")
		require.Zero(t, curbed)
	})

	t.Run("a release clears the key frame mark: the next episode arms at its head", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true)
		sns := r.subSNs(5)
		require.EqualValues(t, sns[4]-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: key frame moved the boundary")

		alloc := r.commit(500_000, s0t2)
		require.False(t, alloc.IsDeficient)
		r.nack(sns[0])
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: not-deficient release")

		r.dt.ForceForwardLayerForTest(s0t2)
		r.writeForwarded(1, false)
		r.writeForwarded(2, false)
		sns = r.subSNs(7)
		r.stepDown(s0t1)
		require.EqualValues(t, sns[6], uint16(r.dt.RTXCurbBoundaryForTest()), "a new episode arms at its own head")
	})

	t.Run("a non-deficient allocation clears the boundary on the next NACK", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)

		alloc := r.commit(500_000, s0t2)
		require.False(t, alloc.IsDeficient)
		require.False(t, alloc.TemporalDownStep)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: the release is lazy")

		rtx, curbed := r.nack(sns[1])
		require.Equal(t, 1, rtx)
		require.Zero(t, curbed)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest())
	})

	t.Run("a second step moves the boundary forward", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		first := r.dt.RTXCurbBoundaryForTest()
		require.NotZero(t, first)

		// the selector reaches the target: current s0t1
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(0, false)
		r.writeForwarded(1, false)
		sns := r.subSNs(6)

		// before the second step the TL1 packet is post-boundary
		rtx, curbed := r.nack(sns[5])
		require.Equal(t, 1, rtx, "precondition: post-step packet repaired")
		require.Zero(t, curbed)

		r.stepDown(s0t0)
		second := r.dt.RTXCurbBoundaryForTest()
		require.Greater(t, second, first)
		require.EqualValues(t, sns[5], uint16(second))

		rtx, curbed = r.nack(sns[5])
		require.Zero(t, rtx)
		require.Equal(t, 1, curbed)
	})

	t.Run("a key frame move during a NACK batch keeps the batch out of the next segment", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, false)
		sns := r.subSNs(5)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: curb active")
		require.Zero(t, r.dt.RTXCurbedForTest(), "precondition: nothing curbed yet")

		before := r.dt.RTXCurbBoundaryForTest()

		// the post-step packet's read stands in for the loop: a key frame moves the
		// curb (and logs its count) while the batch is still running
		r.dh.receiver.ReadRTPCalls(func([]byte, uint8, uint64) (int, error) {
			r.writeForwarded(0, true)
			return 0, io.EOF
		})
		time.Sleep(stepRigOutsideWindow)
		r.dt.RetransmitPacketsForTest([]uint16{sns[1], sns[4]})

		require.Equal(t, 1, r.dh.receiver.ReadRTPCallCount(), "precondition: the batch reached the post-step read")
		after := r.dt.RTXCurbBoundaryForTest()
		require.NotZero(t, after, "a key frame moves the curb, it does not release it")
		require.NotEqual(t, before, after, "precondition: moved mid-batch")
		require.Zero(t, r.dt.RTXCurbedForTest(), "the previous segment's batch must not count toward the next one")
	})

	t.Run("B off: a forwarded key frame releases the curb (base behaviour)", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: false, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)

		rtx, curbed := r.nack(sns[1])
		require.Zero(t, rtx, "precondition: curb active")
		require.Equal(t, 1, curbed)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: boundary set")

		r.writeForwarded(1, false)
		r.writeForwarded(0, true)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "without (B) the key frame releases the curb")
		require.Zero(t, r.dt.RTXCurbedForTest())

		rtx, curbed = r.nack(sns[1])
		require.Equal(t, 1, rtx, "pre-step packet is repaired again after the key frame")
		require.Zero(t, curbed)

		// a later step arms at its own head: no key frame mark was recorded
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(1, false)
		sns = r.subSNs(7)
		r.stepDown(s0t0)
		require.EqualValues(t, sns[6], uint16(r.dt.RTXCurbBoundaryForTest()), "B off: the next step arms at the head")
	})

	t.Run("B off: a release during a NACK batch keeps the batch out of the next episode", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: false, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, false)
		sns := r.subSNs(5)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: curb active")
		require.Zero(t, r.dt.RTXCurbedForTest(), "precondition: nothing curbed yet")

		r.dh.receiver.ReadRTPCalls(func([]byte, uint8, uint64) (int, error) {
			r.writeForwarded(0, true)
			return 0, io.EOF
		})
		time.Sleep(stepRigOutsideWindow)
		r.dt.RetransmitPacketsForTest([]uint16{sns[1], sns[4]})

		require.Equal(t, 1, r.dh.receiver.ReadRTPCallCount(), "precondition: the batch reached the post-step read")
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: released mid-batch")
		require.Zero(t, r.dt.RTXCurbedForTest(), "the released episode's batch must not count toward the next one")
	})

	t.Run("no NACK between episodes: the next episode arms at its own head", func(t *testing.T) {
		// reviewer r1 N1: the earlier episode's key frame mark must not bound a new episode
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(0, true) // K1, the answer to episode 1
		sns := r.subSNs(5)
		require.EqualValues(t, sns[4]-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		// episode 1 ends: the allocation is no longer deficient, no NACK arrives (clean link)
		alloc := r.commit(500_000, s0t2)
		require.False(t, alloc.IsDeficient)
		r.dt.ForceForwardLayerForTest(s0t2)
		r.writeForwarded(1, false)
		r.writeForwarded(2, false)
		r.writeForwarded(0, false)
		sns = r.subSNs(8)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: no NACK, the old boundary lingers")

		// episode 2: a new temporal down-step
		r.stepDown(s0t1)
		require.EqualValues(t, sns[7], uint16(r.dt.RTXCurbBoundaryForTest()), "episode 2 arms at its own head")
		rtx, curbed := r.nack(sns[6])
		require.Zero(t, rtx, "episode 2's pre-step backlog is curbed")
		require.Equal(t, 1, curbed)
	})

	t.Run("a key frame the step already counted as its head still moves the boundary", func(t *testing.T) {
		// reviewer r1 N3 (ii): the commit read the key frame as its head before the key frame branch ran
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)
		head := r.dt.RTXCurbBoundaryForTest()
		require.EqualValues(t, sns[3], uint16(head), "precondition: boundary at the head")
		r.dt.SetRTXCurbBoundaryForTest(head + 1) // the next forwarded packet, K

		r.writeForwarded(0, true)
		sns = r.subSNs(5)
		require.EqualValues(t, head+1, r.dt.RTXCurbBoundaryForTest()+1, "boundary = K - 1")
		require.EqualValues(t, sns[4]-1, uint16(r.dt.RTXCurbBoundaryForTest()))
		rtx, curbed := r.nack(sns[4])
		require.Equal(t, 1, rtx, "K is repairable")
		require.Zero(t, curbed)

		// a key frame older than the boundary (out of order) never lowers it
		r.dt.SetRTXCurbBoundaryForTest(head + 3)
		r.writeForwarded(0, true)
		require.EqualValues(t, head+3, r.dt.RTXCurbBoundaryForTest(), "an older key frame leaves the boundary")
	})

	t.Run("a deficient episode not opened by a temporal step: stale mark cleared, no-curb key frame not recorded", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(0, true) // K1 marks episode 1
		sns := r.subSNs(5)
		require.EqualValues(t, sns[4]-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		// episode 1 ends without a NACK; episode 2 opens with a deficient commit that is not a temporal step
		require.False(t, r.commit(500_000, s0t2).IsDeficient)
		r.dt.ForceForwardLayerForTest(s0t0)
		open := r.commit(150_000, s0t0)
		require.True(t, open.IsDeficient, "precondition: episode 2 is deficient")
		require.False(t, open.TemporalDownStep, "precondition: episode 2 does not open with a temporal step")
		r.nack(sns[0]) // the lazy release only runs when not deficient: the old boundary stays
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: boundary lingers")

		// then the temporal step of episode 2
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(1, false)
		r.writeForwarded(0, false)
		sns = r.subSNs(7)
		r.stepDown(s0t0)
		require.EqualValues(t, sns[6], uint16(r.dt.RTXCurbBoundaryForTest()), "episode 2 arms at its own head")
	})

	t.Run("a key frame forwarded with no curb active is not recorded", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t0})
		r.writeForwarded(0, true)
		r.writeForwarded(0, false)
		r.subSNs(2)
		open := r.commit(150_000, s0t0)
		require.True(t, open.IsDeficient, "precondition: deficient episode")
		require.False(t, open.TemporalDownStep, "precondition: not opened by a temporal step")
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: no curb")

		r.writeForwarded(0, true) // e.g. a layer switch key frame, no curb to move
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(1, false)
		r.writeForwarded(0, false)
		sns := r.subSNs(5)
		r.stepDown(s0t0)
		require.EqualValues(t, sns[4], uint16(r.dt.RTXCurbBoundaryForTest()), "the step arms at its head")
	})

	t.Run("a spatial step never sets the boundary", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, maxSpatial: 1, layer: buffer.VideoLayer{Spatial: 1, Temporal: 2}})
		_, forwarded := r.write(1, 0, true)
		require.True(t, forwarded, "precondition: spatial 1 key frame forwarded")
		_, forwarded = r.write(1, 1, false)
		require.True(t, forwarded, "precondition: spatial 1 packet forwarded")
		r.subSNs(2)

		alloc := r.commit(250_000, s0t1)
		require.True(t, alloc.IsDeficient, "precondition: a deficient spatial down-step")
		require.Equal(t, s0t1, alloc.TargetLayer)
		require.False(t, alloc.TemporalDownStep)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest())
	})
}

// nackTimes NACKs one subscriber sequence number n times and returns the per-NACK rtx and curbed counts.
func (r *stepRig) nackTimes(sn uint16, n int) (rtx []int, curbed []int) {
	for i := 0; i < n; i++ {
		x, c := r.nack(sn)
		rtx = append(rtx, x)
		curbed = append(curbed, c)
	}
	return rtx, curbed
}

// bucket makes every ReadRTP a hit (a bare RTP header: the retransmit stops at the codec header
// size check, so nothing reaches the subscriber) or a miss (not io.EOF, the batch goes on).
func (r *stepRig) bucket(hit bool) {
	r.dh.receiver.ReadRTPCalls(func(buf []byte, _ uint8, _ uint64) (int, error) {
		if !hit {
			return 0, errBucketMiss
		}
		return copy(buf, []byte{0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1}), nil
	})
}

var errBucketMiss = errors.New("bucket miss")

func TestDownTrackRTXCurbKeyFrameRangeUncapped(t *testing.T) {
	t.Run("D1 the key frame is retransmitted beyond maxAck under the curb", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.bucket(true)
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true) // K1
		r.writeForwarded(0, false)
		r.writeForwarded(0, false)
		sns := r.subSNs(7)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")
		require.Zero(t, r.dt.RTXUncappedForTest(), "precondition: nothing served beyond maxAck yet")

		rtx, curbed := r.nackTimes(kf, 4)
		require.Equal(t, []int{1, 1, 1, 1}, rtx, "K1 is repaired beyond maxAck")
		require.Equal(t, []int{0, 0, 0, 0}, curbed)
		require.EqualValues(t, 1, r.dt.RTXUncappedForTest(), "one retransmission beyond maxAck")

		rtx, _ = r.nackTimes(sns[5], 4)
		require.Equal(t, []int{1, 1, 1, 1}, rtx, "packets after K1 are in range too")
		require.EqualValues(t, 2, r.dt.RTXUncappedForTest())

		// a pre-step SN consumes its budget before the curb check: 3 refusals are counted
		rtx, curbed = r.nackTimes(sns[1], 4)
		require.Equal(t, []int{0, 0, 0, 0}, rtx)
		require.Equal(t, []int{1, 1, 1, 0}, curbed)

		// the next key frame move logs and resets the counter
		r.writeForwarded(0, true)
		r.subSNs(8)
		require.Zero(t, r.dt.RTXUncappedForTest(), "the move line reports and resets the uncapped count")
	})

	t.Run("D1w the exemption ends with the key frame repair window", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.bucket(true)
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true) // K1
		r.writeForwarded(0, false)
		sns := r.subSNs(6)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		rtx, _ := r.nackTimes(kf, 3)
		require.Equal(t, []int{1, 1, 1}, rtx, "precondition: K1 at maxAck")
		r.dt.AgeRTXCurbKeyFrameMarkForTest(RTXCurbKeyFrameRepairWindowForTest - 500*time.Millisecond)
		x, _ := r.nack(kf)
		require.Equal(t, 1, x, "inside the window K1 is served beyond maxAck")

		rtx, _ = r.nackTimes(sns[5], 3)
		require.Equal(t, []int{1, 1, 1}, rtx, "precondition: post-K1 packet at maxAck")
		r.dt.AgeRTXCurbKeyFrameMarkForTest(RTXCurbKeyFrameRepairWindowForTest + time.Millisecond)
		x, c := r.nack(sns[5])
		require.Zero(t, x, "after the window the key frame range is back to maxAck")
		require.Zero(t, c, "capped, not curbed: the boundary is unchanged")
		x, _ = r.nack(kf)
		require.Zero(t, x, "K1 too")
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "the curb stays active")
		require.EqualValues(t, 1, r.dt.RTXUncappedForTest(), "only the in-window retransmission is uncapped")
	})

	t.Run("D2 no key frame mark: no exemption", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, false)
		sns := r.subSNs(5)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: curb active")

		rtx, _ := r.nackTimes(sns[4], 4)
		require.Equal(t, []int{1, 1, 1, 0}, rtx, "post-step packet without a key frame mark keeps maxAck")
		require.Zero(t, r.dt.RTXUncappedForTest())
	})

	t.Run("D2b a bucket miss is not counted as uncapped", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.bucket(false)
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true)
		sns := r.subSNs(5)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		rtx, _ := r.nackTimes(kf, 4)
		require.Equal(t, []int{1, 1, 1, 1}, rtx, "precondition: the 4th NACK reaches the bucket")
		require.Zero(t, r.dt.RTXUncappedForTest(), "a miss puts nothing on the wire")
	})

	t.Run("D3 a release clears the exemption and reports the uncapped count", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.bucket(true)
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true)
		r.writeForwarded(0, false)
		sns := r.subSNs(6)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")
		r.nackTimes(sns[5], 4)
		require.EqualValues(t, 1, r.dt.RTXUncappedForTest(), "precondition: one retransmission beyond maxAck")

		require.False(t, r.commit(1_000_000, s0t2).IsDeficient)
		r.nack(sns[0])
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: not-deficient release")
		require.Zero(t, r.dt.RTXUncappedForTest(), "the release line reports and resets the uncapped count")

		rtx, _ := r.nackTimes(kf, 4)
		require.Equal(t, []int{1, 1, 1, 0}, rtx, "after the release K1 is back to maxAck")
	})

	t.Run("D3b a lingering mark without an active curb does not exempt", func(t *testing.T) {
		// a release racing the key frame branch can leave the mark set with no boundary
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true)
		sns := r.subSNs(5)
		kf := sns[4]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")
		r.dt.SetRTXCurbBoundaryForTest(0)

		rtx, _ := r.nackTimes(kf, 4)
		require.Equal(t, []int{1, 1, 1, 0}, rtx, "no curb, no exemption")
		require.Zero(t, r.dt.RTXUncappedForTest())
	})

	t.Run("D4 double step keeps the exemption at K1", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.dt.ForceForwardLayerForTest(s0t1)
		r.writeForwarded(1, false)
		r.writeForwarded(0, true) // K1
		r.writeForwarded(1, false)
		r.writeForwarded(0, false)
		sns := r.subSNs(8)
		kf := sns[5]
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: K1 moved the boundary")

		r.stepDown(s0t0)
		require.EqualValues(t, kf-1, uint16(r.dt.RTXCurbBoundaryForTest()), "precondition: second step stays at K1")

		rtx, curbed := r.nackTimes(kf, 4)
		require.Equal(t, []int{1, 1, 1, 1}, rtx, "K1 stays uncapped after the second step")
		require.Equal(t, []int{0, 0, 0, 0}, curbed)

		x, c := r.nack(sns[4])
		require.Zero(t, x, "post-step pre-K1 packet is curbed")
		require.Equal(t, 1, c)
	})

	t.Run("D5 B off: no mark, no exemption", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: false, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: curb active")
		r.writeForwarded(0, true)
		sns := r.subSNs(5)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: B off, the key frame releases")

		rtx, _ := r.nackTimes(sns[4], 4)
		require.Equal(t, []int{1, 1, 1, 0}, rtx)
		require.Zero(t, r.dt.RTXUncappedForTest())
	})

	t.Run("D5b A and B off (stock): no exemption", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: false, keyFrame: false, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, true)
		sns := r.subSNs(5)

		rtx, _ := r.nackTimes(sns[4], 4)
		require.Equal(t, []int{1, 1, 1, 0}, rtx)
	})
}
