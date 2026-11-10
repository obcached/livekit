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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/mono"

	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
)

const (
	stallLineMsg  = `"msg"="downtrack forward stall"`
	stallEndedMsg = `"msg"="downtrack forward stall ended"`
)

// stallLogRecorder collects the INFO lines of a DownTrack logger.
type stallLogRecorder struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func (r *stallLogRecorder) add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, s)
}
func (r *stallLogRecorder) Logf(format string, args ...any) { r.add(fmt.Sprintf(format, args...)) }
func (r *stallLogRecorder) Log(args ...any)                 { r.add(fmt.Sprint(args...)) }
func (r *stallLogRecorder) Cleanup(f func())                { r.t.Cleanup(f) }

func (r *stallLogRecorder) matching(msg string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, l := range r.lines {
		if strings.Contains(l, msg) {
			out = append(out, l)
		}
	}
	return out
}

type stallScenario struct {
	codec   webrtc.RTPCodecParameters
	fault   *ZeroForwardFault
	unbound bool
	// runs after forwarding started, before the stalled writes
	gate func(dt *DownTrack)
	// stalled writes, true = a late (faulted) arrival
	noStalledWrites bool
}

// newFaultedDownTrack builds the downtrack, sets the fault, then binds it, in the
// order SubscribedTrack uses.
func newFaultedDownTrack(tb testing.TB, params DownTrackParams, fault *ZeroForwardFault, bind bool) *DownTrack {
	tb.Helper()
	dt, _ := newUnwiredDownTrack(tb, params, false)
	dt.SetZeroForwardFault(fault)
	if bind {
		_, err := dt.Bind(&fakeTrackLocalContext{codecs: params.Codecs, ssrc: 0x55667788})
		require.NoError(tb, err)
		dt.SetConnected()
		require.True(tb, dt.IsWritableForTest(), "downtrack should be writable")
	}
	return dt
}

// runStallScenario forwards a few packets, then keeps offering packets that are not
// forwarded, with checks on a 3 s cadence; it returns the recorded lines.
func runStallScenario(t *testing.T, sc stallScenario) *stallLogRecorder {
	t.Helper()
	rec := &stallLogRecorder{t: t}
	if sc.codec.MimeType == "" {
		sc.codec = vp8CodecParams
	}
	if sc.fault == nil {
		sc.fault = NewZeroForwardFault(time.Second)
	}
	dt := newFaultedDownTrack(t, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{sc.codec},
		Logger: logger.NewTestLoggerLevel(rec, 0),
	}, sc.fault, !sc.unbound)
	if !sc.unbound {
		dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	}

	write := func(i int, arrival int64) int32 {
		if sc.codec.MimeType == webrtc.MimeTypeOpus {
			ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{
				SequenceNumber: uint16(1000 + i),
				Timestamp:      uint32(5000 + i*960),
				SSRC:           0x12345678,
				PayloadType:    111,
				PayloadSize:    40,
			})
			require.NoError(t, err)
			ep.Arrival = arrival
			return dt.WriteRTP(ep, 0)
		}
		return dt.WriteRTP(vp8KeyFramePacket(t, i, arrival), 0)
	}

	now := mono.UnixNano()
	forwarded := int32(0)
	for i := 0; i < 5; i++ {
		forwarded += write(i, mono.UnixNano())
	}
	if !sc.unbound {
		require.EqualValues(t, 5, forwarded, "precondition: forwarding before the stall")
	}
	dt.CheckForwardStall(now)

	if sc.gate != nil {
		sc.gate(dt)
	}
	late := mono.UnixNano() + int64(time.Hour)
	for k := 1; k <= 4; k++ {
		if !sc.noStalledWrites {
			for i := 0; i < 5; i++ {
				require.Zero(t, write(5*k+i, late), "stalled write forwarded")
			}
		}
		dt.CheckForwardStall(now + int64(k)*3*int64(time.Second))
	}
	return rec
}

// t1 on a real VP8 DownTrack, with the #5018/#5019 fields on the line
func TestDownTrackForwardStallLine(t *testing.T) {
	rec := runStallScenario(t, stallScenario{})
	lines := rec.matching(stallLineMsg)
	require.Len(t, lines, 1, "one onset line")
	for _, field := range []string{
		`"line"="onset"`, `"stallMs"=6000`, `"offeredDelta"=10`, `"forwardedDelta"=0`, `"fault"=true`,
		`"mime"="video/VP8"`, `"bindState"`, `"targetLayer"`, `"currentLayer"`, `"maxLayer"`, `"isDeficient"`,
		`"syncLocked"`, `"syncLayer"`, `"keyFrameRequesterWakeups"`, `"rtt"`, `"lastKeyFrameFwdAgoMs"`,
		`"lastPliAgoMs"`, `"droppedDelta"`, `"droppedLines"`, `"dummyStartTSOffset"`, `"referenceLayerSpatial"`,
		`"extFirstTS"`, `"mungerLastExtSN"`, `"mungerLastExtTS"`, `"srLayers"`, `"lastSSRC"`,
		`"lastSwitchExtIncomingTS"`, `"lastSwitchReject"="none"`, `"switchRejects"=0`, `"lastSwitchRejectAgoMs"=-1`,
	} {
		require.Contains(t, lines[0], field)
	}
	require.Empty(t, rec.matching(stallEndedMsg))
}

func TestDownTrackForwardStallEndedLineWhenClosed(t *testing.T) {
	rec := &stallLogRecorder{t: t}
	dt := newFaultedDownTrack(t, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
		Logger: logger.NewTestLoggerLevel(rec, 0),
	}, NewZeroForwardFault(time.Second), true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	now := mono.UnixNano()
	require.EqualValues(t, 1, dt.WriteRTP(vp8KeyFramePacket(t, 0, now), 0))
	dt.CheckForwardStall(now)
	late := now + int64(time.Hour)
	for k := 1; k <= 2; k++ {
		dt.WriteRTP(vp8KeyFramePacket(t, k, late), 0)
		dt.CheckForwardStall(now + int64(k)*3*int64(time.Second))
	}
	require.Len(t, rec.matching(stallLineMsg), 1, "precondition: onset logged")
	require.Empty(t, rec.matching(stallEndedMsg), "precondition: no ended line before close")

	// the subscriber's RTCP worker drops a closed downtrack before its next tick, so the
	// ended line must come from the close itself
	dt.Close()
	ended := rec.matching(stallEndedMsg)
	require.Len(t, ended, 1, "close emits the ended line")
	require.Contains(t, ended[0], `"closed"=true`)

	// a later check (a tick racing the close) does not log it again
	dt.CheckForwardStall(now + 12*int64(time.Second))
	require.Len(t, rec.matching(stallEndedMsg), 1)
}

// a downtrack closed without an open stall logs no ended line
func TestDownTrackForwardStallNoEndedLineWhenClosedWithoutStall(t *testing.T) {
	rec := &stallLogRecorder{t: t}
	dt := newFaultedDownTrack(t, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
		Logger: logger.NewTestLoggerLevel(rec, 0),
	}, nil, true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	now := mono.UnixNano()
	require.EqualValues(t, 1, dt.WriteRTP(vp8KeyFramePacket(t, 0, now), 0), "precondition: forwarding")
	dt.CheckForwardStall(now)
	dt.Close()
	require.Empty(t, rec.matching(stallLineMsg))
	require.Empty(t, rec.matching(stallEndedMsg))
}

// a downtrack that never forwarded has no sender report (the RTCP worker skips it after
// the check), and still gets its stall line, counted from bind
func TestDownTrackForwardStallNeverForwarded(t *testing.T) {
	rec := &stallLogRecorder{t: t}
	dt := newFaultedDownTrack(t, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
		Logger: logger.NewTestLoggerLevel(rec, 0),
	}, NewZeroForwardFault(time.Nanosecond), true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	now := mono.UnixNano()
	late := now + int64(time.Hour)
	for k := 0; k <= 2; k++ {
		require.Zero(t, dt.WriteRTP(vp8KeyFramePacket(t, k, late), 0))
		require.Nil(t, dt.CreateSenderReport(), "precondition: no sender report")
		dt.CheckForwardStall(now + int64(k)*3*int64(time.Second))
	}
	lines := rec.matching(stallLineMsg)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], `"forwarderStarted"=false`)
}

// packets the forwarder drops (here: a layer other than the target) count as offered
// and dropped, without any fault
func TestDownTrackForwardStallCountsForwarderDrops(t *testing.T) {
	rec := &stallLogRecorder{t: t}
	dt, _ := newUnwiredDownTrack(t, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
		Logger: logger.NewTestLoggerLevel(rec, 0),
	}, true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	now := mono.UnixNano()
	require.EqualValues(t, 1, dt.WriteRTP(vp8KeyFramePacket(t, 0, now), 0))
	dt.CheckForwardStall(now)
	for k := 1; k <= 2; k++ {
		for i := 0; i < 4; i++ {
			ep := vp8KeyFramePacket(t, 10*k+i, now)
			ep.Packet.SSRC = 0x44444444
			require.Zero(t, dt.WriteRTP(ep, 1), "precondition: non-target layer dropped")
		}
		dt.CheckForwardStall(now + int64(k)*3*int64(time.Second))
	}
	lines := rec.matching(stallLineMsg)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], `"offeredDelta"=8`)
	require.Contains(t, lines[0], `"droppedDelta"=8`)
	require.Contains(t, lines[0], `"fault"=false`)
}

// t3, t4, t5, t6, t7, t13: each gate silences the line that its twin (baseline) emits
func TestDownTrackForwardStallGates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sc    stallScenario
		lines int
	}{
		{name: "baseline", lines: 1},
		{name: "muted", sc: stallScenario{gate: func(dt *DownTrack) { dt.Mute(true) }}},
		{name: "pubMuted", sc: stallScenario{gate: func(dt *DownTrack) { dt.PubMute(true) }}},
		{name: "unbound", sc: stallScenario{unbound: true}},
		{name: "upstreamIdle", sc: stallScenario{noStalledWrites: true}},
		{name: "audio", sc: stallScenario{codec: opusCodecParams}},
		{name: "invalidTarget", sc: stallScenario{gate: func(dt *DownTrack) { dt.ForceForwardLayerForTest(buffer.InvalidLayer) }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := runStallScenario(t, tc.sc)
			require.Len(t, rec.matching(stallLineMsg), tc.lines)
		})
	}
}

// t11: the fault drops every packet once active, and only on the downtrack it is set on
func TestZeroForwardFaultDropsOnlyWhenSet(t *testing.T) {
	newDT := func(fault *ZeroForwardFault) *DownTrack {
		dt := newFaultedDownTrack(t, DownTrackParams{
			Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
		}, fault, true)
		dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})
		return dt
	}

	first := ZeroForwardFaultFor("-czf", 2*time.Second, true, "t11-alice-czf", "TR_t11")
	second := ZeroForwardFaultFor("-czf", 2*time.Second, true, "t11-alice-czf", "TR_t11")
	require.NotNil(t, first, "first downtrack is faulted")
	require.Nil(t, second, "replacement downtrack is clean")

	faulted, clean := newDT(first), newDT(second)
	now := mono.UnixNano()
	late := now + int64(time.Minute)
	require.EqualValues(t, 1, faulted.WriteRTP(vp8KeyFramePacket(t, 0, now), 0), "before after_seconds")
	require.EqualValues(t, 0, faulted.WriteRTP(vp8KeyFramePacket(t, 1, late), 0), "after after_seconds")
	require.EqualValues(t, 0, faulted.WriteRTP(vp8KeyFramePacket(t, 2, late), 0))

	// key absent: nil pointer, zero faulted packets however long after bind
	for i := 0; i < 3; i++ {
		require.EqualValues(t, 1, clean.WriteRTP(vp8KeyFramePacket(t, i, late+int64(i)), 0))
	}
}

func TestZeroForwardFaultForSelection(t *testing.T) {
	require.Nil(t, ZeroForwardFaultFor("-czf", time.Second, true, "t11-bob", "TR_a"), "suffix mismatch")
	require.Nil(t, ZeroForwardFaultFor("-czf", time.Second, false, "t11-bob-czf", "TR_a"), "audio")
	require.Nil(t, ZeroForwardFaultFor("", time.Second, true, "t11-bob-czf", "TR_a"), "empty suffix")
	require.Nil(t, ZeroForwardFaultFor("-czf", 0, true, "t11-bob-czf", "TR_a"), "after not positive")

	require.NotNil(t, ZeroForwardFaultFor("-czf", time.Second, true, "t11-bob-czf", "TR_a"))
	require.Nil(t, ZeroForwardFaultFor("-czf", time.Second, true, "t11-bob-czf", "TR_a"))
	require.NotNil(t, ZeroForwardFaultFor("-czf", time.Second, true, "t11-bob-czf", "TR_b"), "another track")
	require.NotNil(t, ZeroForwardFaultFor("-czf", time.Second, true, "t11-carol-czf", "TR_a"), "another subscriber")
}

func BenchmarkDownTrackWriteRTPHookOnInactive(b *testing.B) {
	dt := newFaultedDownTrack(b, DownTrackParams{
		Codecs: []webrtc.RTPCodecParameters{vp8CodecParams},
	}, NewZeroForwardFault(1000*time.Hour), true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})

	ep := vp8KeyFramePacket(b, 0, 1)
	require.Equal(b, int32(1), dt.WriteRTP(ep, 0), "first packet should forward")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ep.Packet.SequenceNumber++
		ep.ExtSequenceNumber++
		ep.Packet.Timestamp += 3000
		ep.ExtTimestamp += 3000
		ep.Arrival++
		dt.WriteRTP(ep, 0)
	}
}
