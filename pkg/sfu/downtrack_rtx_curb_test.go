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

	t.Run("a forwarded key frame clears the boundary", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, layer: s0t2})
		sns := r.preStep()
		r.stepDown(s0t1)

		rtx, curbed := r.nack(sns[1])
		require.Zero(t, rtx, "precondition: curb active")
		require.Equal(t, 1, curbed)

		r.writeForwarded(0, true)
		require.Zero(t, r.dt.RTXCurbBoundaryForTest())

		rtx, curbed = r.nack(sns[1])
		require.Equal(t, 1, rtx, "pre-step packet is repaired again after the key frame")
		require.Zero(t, curbed)
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

	t.Run("a release during a NACK batch keeps the batch out of the next episode", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, layer: s0t2})
		r.preStep()
		r.stepDown(s0t1)
		r.writeForwarded(0, false)
		sns := r.subSNs(5)
		require.NotZero(t, r.dt.RTXCurbBoundaryForTest(), "precondition: curb active")
		require.Zero(t, r.dt.RTXCurbedForTest(), "precondition: nothing curbed yet")

		// the post-step packet's read stands in for the loop: a key frame releases the
		// curb (and logs its count) while the batch is still running
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
