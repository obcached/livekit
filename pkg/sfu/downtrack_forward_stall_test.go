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

package sfu

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const stallSec = int64(time.Second)

// stallFeed drives a forwardStallState the way the RTCP worker does: one sample per
// check, at arbitrary mono times.
type stallFeed struct {
	s         forwardStallState
	boundAt   int64
	offered   uint64
	dropped   uint64
	forwarded uint64
}

func (f *stallFeed) check(now int64, eligible bool) forwardStallResult {
	return f.s.step(forwardStallSample{
		eligible:  eligible,
		offered:   f.offered,
		dropped:   f.dropped,
		forwarded: f.forwarded,
	}, now, f.boundAt)
}

// t1: stalled for 5.1 s -> one onset line
func TestForwardStallOnsetAfterFiveSeconds(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	t0 := f.boundAt + 2*stallSec

	f.offered, f.forwarded = 100, 90
	require.Equal(t, forwardStallNone, f.check(t0, true).event, "forwarding check")

	f.offered += 50
	f.dropped += 7
	r := f.check(t0+5100*int64(time.Millisecond), true)
	require.Equal(t, forwardStallOnsetLine, r.event)
	require.Equal(t, 5100*int64(time.Millisecond), r.stallNanos)
	require.EqualValues(t, 50, r.offeredDelta)
	require.EqualValues(t, 7, r.droppedDelta)
	require.EqualValues(t, 0, r.forwardedDelta)
}

// t2: stalled for 4.9 s -> nothing, and no ended line when forwarding resumes
func TestForwardStallNoLineBelowFiveSeconds(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	t0 := f.boundAt + 2*stallSec

	f.offered, f.forwarded = 100, 90
	require.Equal(t, forwardStallNone, f.check(t0, true).event)

	f.offered += 50
	require.Equal(t, forwardStallNone, f.check(t0+4900*int64(time.Millisecond), true).event)

	f.offered += 50
	f.forwarded += 10
	require.Equal(t, forwardStallNone, f.check(t0+6*stallSec, true).event, "no ended line without an onset")
}

// a downtrack that never forwarded since bind stalls from its bind time
func TestForwardStallNeverForwardedCountsFromBind(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}

	f.offered = 10
	require.Equal(t, forwardStallNone, f.check(f.boundAt+3*stallSec, true).event)
	f.offered = 20
	r := f.check(f.boundAt+6*stallSec, true)
	require.Equal(t, forwardStallOnsetLine, r.event)
	require.Equal(t, 6*stallSec, r.stallNanos)
	require.EqualValues(t, 20, r.offeredDelta)
}

// t1b: 3 s cadence, forwarding stops at T0, the first check after T0 runs at T0 + phi:
// the onset line comes at T0 + phi + 6 s, never earlier
func TestForwardStallOnsetBoundOnThreeSecondCadence(t *testing.T) {
	for _, phi := range []time.Duration{0, 100 * time.Millisecond, 1500 * time.Millisecond, 2900 * time.Millisecond} {
		t.Run(phi.String(), func(t *testing.T) {
			f := &stallFeed{boundAt: 1_000 * stallSec}
			t0 := f.boundAt + 30*stallSec
			first := t0 + int64(phi)

			// packets arrive every 10 ms; forwarded ones only up to and including T0
			at := func(now int64) {
				f.offered = uint64((now - f.boundAt) / int64(10*time.Millisecond))
				f.forwarded = uint64((min(now, t0) - f.boundAt) / int64(10*time.Millisecond))
			}

			var onsetAt int64
			for now := first - 9*3*stallSec; now <= first+12*stallSec; now += 3 * stallSec {
				at(now)
				r := f.check(now, true)
				if r.event == forwardStallOnsetLine {
					onsetAt = now
					require.Greater(t, r.stallNanos, 5*stallSec)
					break
				}
				require.Equal(t, forwardStallNone, r.event, "no line before the onset, now-T0 %s", time.Duration(now-t0))
			}
			require.Equal(t, first+6*stallSec, onsetAt)
			require.GreaterOrEqual(t, onsetAt-t0, 6*stallSec)
			require.Less(t, onsetAt-t0, 9*stallSec)
		})
	}
}

// t6: upstream idle (nothing offered) -> no line however long forwarded stays flat
func TestForwardStallUpstreamIdleNoLine(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	f.offered, f.forwarded = 100, 90
	for now := f.boundAt + 3*stallSec; now < f.boundAt+120*stallSec; now += 3 * stallSec {
		require.Equal(t, forwardStallNone, f.check(now, true).event)
	}
}

// t3/t4/t5/t7/t13 at the core: a gated check resets the stall start
func TestForwardStallGatedCheckResets(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	now := f.boundAt
	for i := 0; i < 20; i++ {
		now += 3 * stallSec
		f.offered += 100
		require.Equal(t, forwardStallNone, f.check(now, false).event, "gated check %d", i)
	}

	// gate opens: the stall counts from the last gated check, not from bind
	now += 3 * stallSec
	f.offered += 100
	require.Equal(t, forwardStallNone, f.check(now, true).event)
	now += 3 * stallSec
	f.offered += 100
	r := f.check(now, true)
	require.Equal(t, forwardStallOnsetLine, r.event)
	require.Equal(t, 6*stallSec, r.stallNanos)
}

// t8: repeat lines at most every 30 s, at most 10 lines per downtrack
func TestForwardStallRepeatRateAndCap(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	f.offered, f.forwarded = 100, 90
	require.Equal(t, forwardStallNone, f.check(f.boundAt+stallSec, true).event)

	var lines []int64
	for now := f.boundAt + 4*stallSec; now < f.boundAt+1200*stallSec; now += 3 * stallSec {
		f.offered += 100
		r := f.check(now, true)
		switch r.event {
		case forwardStallOnsetLine:
			require.Empty(t, lines, "a single onset")
			lines = append(lines, now)
		case forwardStallRepeatLine:
			require.NotEmpty(t, lines)
			require.GreaterOrEqual(t, now-lines[len(lines)-1], 30*stallSec)
			lines = append(lines, now)
		default:
			require.Equal(t, forwardStallNone, r.event)
		}
	}
	require.Len(t, lines, forwardStallMaxLines)

	// a later stall on the same downtrack gets no line once the cap is spent
	f.forwarded++
	f.offered += 100
	now := f.boundAt + 1300*stallSec
	require.Equal(t, forwardStallEnded, f.check(now, true).event)
	for i := 0; i < 10; i++ {
		now += 3 * stallSec
		f.offered += 100
		require.Equal(t, forwardStallNone, f.check(now, true).event)
	}
}

// t9: one ended line when forwarding resumes, none after
func TestForwardStallEndedOnce(t *testing.T) {
	f := &stallFeed{boundAt: 1_000 * stallSec}
	f.offered, f.forwarded = 100, 90
	now := f.boundAt + stallSec
	f.check(now, true)
	for i := 0; i < 3; i++ {
		now += 3 * stallSec
		f.offered += 100
		f.check(now, true)
	}

	now += 3 * stallSec
	f.offered += 100
	f.forwarded += 50
	r := f.check(now, true)
	require.Equal(t, forwardStallEnded, r.event)
	require.Equal(t, 12*stallSec, r.stallNanos)

	for i := 0; i < 3; i++ {
		now += 3 * stallSec
		f.offered += 100
		f.forwarded += 50
		require.Equal(t, forwardStallNone, f.check(now, true).event)
	}

	// a gate closing (closed, muted, unbound) also ends a logged stall, once
	for i := 0; i < 3; i++ {
		now += 3 * stallSec
		f.offered += 100
		f.check(now, true)
	}
	now += 3 * stallSec
	require.Equal(t, forwardStallEnded, f.check(now, false).event)
	now += 3 * stallSec
	require.Equal(t, forwardStallNone, f.check(now, false).event)
}

// t10: process-wide token bucket, 10 lines/s, drops counted
func TestForwardStallLimiterDropsAndCounts(t *testing.T) {
	l := newForwardStallLimiter()
	now := 50 * stallSec
	for i := 0; i < forwardStallLinesPerSecond; i++ {
		ok, dropped := l.allow(now)
		require.True(t, ok, "line %d", i)
		require.Zero(t, dropped)
	}
	ok, dropped := l.allow(now)
	require.False(t, ok)
	require.EqualValues(t, 1, dropped)
	ok, dropped = l.allow(now + 50*int64(time.Millisecond))
	require.False(t, ok)
	require.EqualValues(t, 2, dropped)

	// refilled after a second, the drop count stays cumulative
	ok, dropped = l.allow(now + stallSec)
	require.True(t, ok)
	require.EqualValues(t, 2, dropped)
}

// ---------------------------------------------------------------------------
// forwarder switch-reject observation

func fieldsMap(t *testing.T, kv []any) map[string]any {
	t.Helper()
	require.Zero(t, len(kv)%2)
	m := make(map[string]any, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		k, ok := kv[i].(string)
		require.True(t, ok, "key %v", kv[i])
		m[k] = kv[i+1]
	}
	return m
}

// t12: a switch point too far behind is recorded and reaches the stall line fields
func TestForwarderRecordsSwitchRejectTooFarBehind(t *testing.T) {
	f := newWrapTestForwarder(t, false)
	lowTS, cycles := runToHighLayer(t, f, 0x1000, 10*time.Minute)
	require.Zero(t, f.SwitchDebug().Rejects, "precondition: no reject yet")

	// layer 0 comes back with a key frame 2 s behind what was last sent
	behind := uint64(lowTS) + uint64(cycles)<<32 - 2*90000
	pkt := wrapTestKeyFrame(t, wrapTestSSRCLow, 1001, uint32(behind), int(behind>>32), 0)
	const arrival = int64(777 * time.Second)
	pkt.Arrival = arrival
	tp, err := f.GetTranslationParams(pkt, 0)
	require.NoError(t, err)
	require.True(t, tp.shouldDrop, "precondition: the switch is rejected")

	sd := f.SwitchDebug()
	require.Equal(t, SwitchRejectTooFarBehind, sd.LastReject)
	require.GreaterOrEqual(t, sd.Rejects, uint32(1))
	require.Equal(t, arrival, sd.LastRejectAt)
	require.Equal(t, wrapTestSSRCHigh, sd.LastSSRC)

	m := fieldsMap(t, appendForwarderStallFields(nil, f, arrival+2*stallSec))
	require.Equal(t, "too_far_behind", m["lastSwitchReject"])
	require.EqualValues(t, sd.Rejects, m["switchRejects"])
	require.EqualValues(t, 2000, m["lastSwitchRejectAgoMs"])
	require.Equal(t, int64(f.dummyStartTSOffset), m["dummyStartTSOffset"])
	require.Contains(t, m, "srLayers")
	require.Contains(t, fmt.Sprint(m["srLayers"]), "0:")
	require.Contains(t, m, "referenceLayerSpatial")
	require.Contains(t, m, "extFirstTS")
	require.Contains(t, m, "mungerLastExtSN")
	require.Contains(t, m, "mungerLastExtTS")
	require.Contains(t, m, "lastSSRC")
	require.Contains(t, m, "lastSwitchExtIncomingTS")
}

// t12b: the v1.13.11 wrap fixture switches back without a single reject
func TestForwarderNoSwitchRejectAfterReferenceTimestampWrap(t *testing.T) {
	f := newWrapTestForwarder(t, false)
	lowTS, cycles := runToHighLayer(t, f, uint32(0xf0000000), time.Hour)
	require.Less(t, int64(f.dummyStartTSOffset), int64(0), "precondition: negative dummy offset")
	require.Equal(t, 1, cycles, "precondition: layer 0 wrapped")

	tp, err := f.GetTranslationParams(wrapTestKeyFrame(t, wrapTestSSRCLow, 1001, lowTS, cycles, 0), 0)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop)
	require.Zero(t, f.SwitchDebug().Rejects)
	require.Equal(t, SwitchRejectNone, f.SwitchDebug().LastReject)

	m := fieldsMap(t, appendForwarderStallFields(nil, f, 0))
	require.Equal(t, "none", m["lastSwitchReject"])
	require.EqualValues(t, -1, m["lastSwitchRejectAgoMs"])
}

// a layer without a sender report is a reference timestamp reject
func TestForwarderRecordsSwitchRejectRefTSUnavailable(t *testing.T) {
	f := newWrapTestForwarder(t, false)
	f.maybeStart()
	f.AllocateOptimal([]int32{0}, wrapTestBitrates(0), true, false, 0)
	tp, err := f.GetTranslationParams(wrapTestKeyFrame(t, wrapTestSSRCLow, 1000, 0x1000, 0, 0), 0)
	require.NoError(t, err)
	require.False(t, tp.shouldDrop)

	f.AllocateOptimal([]int32{2}, wrapTestBitrates(2), true, false, 0)
	tp, err = f.GetTranslationParams(wrapTestKeyFrame(t, wrapTestSSRCHigh, 5000, wrapTestHighTS, 0, 2), 2)
	require.NoError(t, err)
	require.True(t, tp.shouldDrop, "precondition: no sender report for the layers")

	sd := f.SwitchDebug()
	require.Equal(t, SwitchRejectRefTSUnavailable, sd.LastReject)
	require.EqualValues(t, 1, sd.Rejects)
}

func TestSwitchRejectReasonNames(t *testing.T) {
	require.Equal(t, SwitchRejectSkipStartOutOfOrder, switchRejectReasonOf(errSkipStartOnOutOfOrderPacket))
	require.Equal(t, SwitchRejectTooFarBehind, switchRejectReasonOf(errSwitchPointTooFarBehind))
	require.Equal(t, SwitchRejectRefTSUnavailable, switchRejectReasonOf(fmt.Errorf("unavailable layer ref")))
	require.Equal(t, SwitchRejectOther, switchRejectReasonOf(nil))
	for r, name := range map[SwitchRejectReason]string{
		SwitchRejectNone:                "none",
		SwitchRejectSkipStartOutOfOrder: "skip_start_ooo",
		SwitchRejectRefTSUnavailable:    "ref_ts_unavailable",
		SwitchRejectTooFarBehind:        "too_far_behind",
		SwitchRejectOther:               "other",
	} {
		require.Equal(t, name, r.String())
	}
}
