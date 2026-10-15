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

package sendsidebwe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/sfu/bwe"
)

// A subscriber whose receive clock runs fast by r ppm relative to the SFU adds r us of
// apparent one-way delay per second, with no queue at all. The propagated queuing delay is
// an unanchored integral of (recvDelta - sendDelta), so without a re-anchor it walks past
// the JQR min delay (50 ms) after 50 ms / r and the detector goes CONGESTED with zero loss,
// then never probes (CanProbe needs NONE). Observed in prod (room 01a13e5f, 2026-10-15):
// +23 ppm -> CONGESTED after ~36 min, +33 ppm after ~26 min. The windowed-minimum baseline
// (queuing_delay_baseline_window) caps the drift's contribution at r * window.
func TestQueuingDelayBaselineWindowClockDrift(t *testing.T) {
	const drift = 45 * time.Minute
	jqrMinDelay := defaultCongestionDetectorConfig.JQRMinDelay

	cases := []struct {
		name   string
		window time.Duration
		ppm    float64
		// zero = must stay NONE for the whole run
		congestedMin, congestedMax time.Duration
	}{
		{name: "window off, 0 ppm stays NONE", ppm: 0},
		{name: "window off, -33 ppm (slow receive clock) stays NONE", ppm: -33},
		{name: "window off, +23 ppm goes CONGESTED", ppm: 23, congestedMin: 30 * time.Minute, congestedMax: 42 * time.Minute},
		{name: "window off, +33 ppm goes CONGESTED", ppm: 33, congestedMin: 20 * time.Minute, congestedMax: 31 * time.Minute},
		{name: "window 10s, +33 ppm stays NONE", window: 10 * time.Second, ppm: 33},
		{name: "window 30s, +33 ppm stays NONE", window: 30 * time.Second, ppm: 33},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newQDSim(t, c.window)
			var maxRaw int64
			// the sim never prunes (pruning is wall-clock keyed), so scan only the newest groups:
			// a full scan per report is O(n^2) over ~28k groups and too slow under -race
			s.onReport = func() {
				for _, pg := range s.c.packetGroups[max(0, len(s.c.packetGroups)-4):] {
					if raw, ok := pg.FinalizedPropagatedQueuingDelay(); ok {
						maxRaw = max(maxRaw, raw)
					}
				}
			}

			// the harness needs owd >= 0: a slow receive clock starts high and walks down
			start := time.Duration(0)
			if c.ppm < 0 {
				start = 100 * time.Millisecond
			}
			s.run(qdSimGroups(5*time.Second), flatOWD(start), 0)
			driftStart := s.now
			s.run(qdSimGroups(drift), rampOWD(start, time.Duration(c.ppm*float64(time.Microsecond))), 0)

			if c.ppm > 0 {
				require.Greater(t, maxRaw, jqrMinDelay.Microseconds(), "precondition: the raw pqd drifted past the JQR min delay")
			} else {
				require.Less(t, maxRaw, jqrMinDelay.Microseconds(), "precondition: no drift past the JQR min delay")
			}

			if c.congestedMax == 0 {
				require.Empty(t, s.transitions, "left NONE")
				require.Equal(t, bwe.CongestionStateNone, s.c.CongestionState())
				require.True(t, s.c.CanProbe())
				return
			}

			congested, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
			require.True(t, ok, "CONGESTED expected from drift alone")
			require.Equal(t, congestionReasonQueuingDelay, congested.reason)
			after := congested.at - driftStart
			require.GreaterOrEqual(t, after, c.congestedMin)
			require.LessOrEqual(t, after, c.congestedMax)
			// stuck: NONE -> EARLY_WARNING -> CONGESTED and nothing after, no probe, until the end of the run
			require.Len(t, s.transitions, 2, "flapped after CONGESTED")
			require.Equal(t, bwe.CongestionStateCongested, s.c.CongestionState())
			require.False(t, s.c.CanProbe())
		})
	}
}

// KNOWN RISK of the baseline window, documented as current behaviour: the effective queuing
// delay of a linear ramp of slope r is ~r * window, so a genuine queue growing slower than
// JQR min delay / window (5 ms/s at 10 s, ~1.67 ms/s at 30 s) never crosses the JQR min delay
// and is hidden. A real +3 ms/s queue reaches 180 ms one-way delay over 60 s: CONGESTED without
// the window or with a 30 s window, NONE with a 10 s window. A +1.5 ms/s queue is hidden even
// by the 30 s window. Loss detection still applies once the queue overflows.
func TestQueuingDelayBaselineWindowHidesSlowRealQueue(t *testing.T) {
	feed := func(t *testing.T, window, slope time.Duration) *qdSim {
		s := newQDSim(t, window)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		s.run(qdSimGroups(60*time.Second), rampOWD(0, slope), 0)
		return s
	}
	detected := func(t *testing.T, s *qdSim) {
		congested, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
		require.True(t, ok, "the slow real queue must go CONGESTED")
		require.Equal(t, congestionReasonQueuingDelay, congested.reason)
		require.Equal(t, bwe.CongestionStateCongested, s.c.CongestionState())
	}
	hidden := func(t *testing.T, s *qdSim) {
		require.False(t, s.reached(bwe.CongestionStateCongested), "known risk: slow real queue hidden by the window")
		require.Equal(t, bwe.CongestionStateNone, s.c.CongestionState())
	}

	t.Run("window off detects 3 ms/s", func(t *testing.T) {
		detected(t, feed(t, 0, 3*time.Millisecond))
	})

	t.Run("window 10s hides 3 ms/s", func(t *testing.T) {
		hidden(t, feed(t, 10*time.Second, 3*time.Millisecond))
	})

	t.Run("window 30s detects 3 ms/s", func(t *testing.T) {
		detected(t, feed(t, 30*time.Second, 3*time.Millisecond))
	})

	t.Run("window off detects 1.5 ms/s", func(t *testing.T) {
		detected(t, feed(t, 0, 1500*time.Microsecond))
	})

	t.Run("window 30s hides 1.5 ms/s", func(t *testing.T) {
		hidden(t, feed(t, 30*time.Second, 1500*time.Microsecond))
	})
}
