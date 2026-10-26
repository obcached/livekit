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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu/bwe"
)

type slRecordingLogger struct {
	logger.Logger

	mu    sync.Mutex
	infos []string
}

func (l *slRecordingLogger) Infow(msg string, keysAndValues ...any) {
	l.mu.Lock()
	l.infos = append(l.infos, msg)
	l.mu.Unlock()
	l.Logger.Infow(msg, keysAndValues...)
}

func (l *slRecordingLogger) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0
	for _, msg := range l.infos {
		if strings.HasPrefix(msg, prefix) {
			n++
		}
	}
	return n
}

const slTrendLine = "send side bwe: captured traffic ratio is trending downward"

func newSLDetector(minCTR float64, l logger.Logger) *congestionDetector {
	if l == nil {
		l = logger.GetLogger()
	}
	cfg := defaultCongestionDetectorConfig
	cfg.SenderLimitedMinCTR = minCTR
	return newCongestionDetector(congestionDetectorParams{Config: cfg, Logger: l})
}

// slTrafficStats: one second of traffic, ackedBytes 125000 (1 Mbps before the CTR factor).
func slTrafficStats(sendDelta, recvDelta int64, lostPackets int) *trafficStats {
	return &trafficStats{
		params:       trafficStatsParams{Config: defaultWeightedLossConfig, Logger: logger.GetLogger()},
		minSendTime:  1,
		maxSendTime:  1_000_001,
		sendDelta:    sendDelta,
		recvDelta:    recvDelta,
		ackedPackets: 100 - lostPackets,
		ackedBytes:   125000,
		lostPackets:  lostPackets,
		lostBytes:    lostPackets * 1250,
	}
}

// U1: the predicate. Mutants: `>=` -> `>`; loss rule dropped.
func TestSenderLimitedPredicate(t *testing.T) {
	lossy := slTrafficStats(1100, 1000, 1)
	require.Greater(t, lossy.WeightedLoss(), 0.0, "precondition: lossy stats carry weighted loss")
	require.GreaterOrEqual(t, lossy.CapturedTrafficRatio(), 0.99, "precondition: lossy stats keep CTR >= key")

	exact := slTrafficStats(990, 1000, 0)
	require.Equal(t, 0.99, exact.CapturedTrafficRatio(), "precondition: boundary CTR is exactly the key")

	cases := []struct {
		name string
		key  float64
		ts   *trafficStats
		want bool
	}{
		{"ctr 0.999 no loss", 0.99, slTrafficStats(999, 1000, 0), true},
		{"ctr 0.985 no loss", 0.99, slTrafficStats(985, 1000, 0), false},
		{"ctr 1.0 with loss", 0.99, lossy, false},
		{"ctr == key", 0.99, exact, true},
		{"nil stats", 0.99, nil, false},
		{"key 0, ctr 0.999", 0, slTrafficStats(999, 1000, 0), false},
		{"key 0, ctr == 1", 0, slTrafficStats(1000, 1000, 0), false},
		{"key 0, lossy", 0, lossy, false},
		{"key 0, nil", 0, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, newSLDetector(c.key, nil).senderLimited(c.ts))
		})
	}
}

// forceCongestedWithDownwardTrend puts the detector in CONGESTED with a CTR trend that reads downward and the given
// aggregate congested traffic stats.
func forceCongestedWithDownwardTrend(t *testing.T, c *congestionDetector, ts *trafficStats, estimate int64) {
	t.Helper()

	c.congestionState = bwe.CongestionStateCongested
	c.createCTRTrend()
	for _, v := range []float64{1.0, 0.95, 0.90, 0.85} {
		c.congestedCTRTrend.AddValue(v)
	}
	c.congestedTrafficStats = ts
	c.estimatedAvailableChannelCapacity = estimate
}

// U-a1gone: a sender-limited downward CTR trend lowers the estimate and notifies like stock, with zero evidence
// (transitions only). Mutants: the A-1 guard re-inserted (not lowered); the trend-fire evidence copy re-inserted
// (SenderLimited true).
func TestSenderLimitedTrendNotIgnored(t *testing.T) {
	const estimate = int64(2_000_000)

	for _, key := range []float64{0, 0.99} {
		t.Run(fmt.Sprintf("key %v", key), func(t *testing.T) {
			l := &slRecordingLogger{Logger: logger.GetLogger()}
			d := newSLDetector(key, l)
			forceCongestedWithDownwardTrend(t, d, slTrafficStats(1000, 1000, 0), estimate)
			acked := d.congestedTrafficStats.AcknowledgedBitrate()
			require.Less(t, acked, estimate, "precondition: acked < estimate")
			require.Equal(t, key > 0, d.senderLimited(d.congestedTrafficStats), "precondition: sender limited iff key > 0")
			trendBefore := d.congestedCTRTrend

			notify, from, to, got, ev := d.congestionDetectionStateMachine()
			require.True(t, notify)
			require.Equal(t, bwe.CongestionStateCongested, from)
			require.Equal(t, bwe.CongestionStateCongested, to)
			require.Equal(t, acked, got)
			require.Equal(t, acked, d.estimatedAvailableChannelCapacity)
			require.Equal(t, bwe.CongestionEvidence{}, ev)
			require.Equal(t, 1, l.count(slTrendLine))

			require.NotSame(t, trendBefore, d.congestedCTRTrend)
			require.False(t, d.congestedCTRTrend.HasEnoughSamples())
		})
	}
}

// U3: A-1 does not block a real undershoot. Mutant: guard ignores CTR (or loss).
func TestSenderLimitedTrendRealUndershoot(t *testing.T) {
	const estimate = int64(2_000_000)

	for _, c := range []struct {
		name string
		ts   *trafficStats
	}{
		{"ctr 0.90", slTrafficStats(900, 1000, 0)},
		{"lossy", slTrafficStats(1100, 1000, 1)},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := &slRecordingLogger{Logger: logger.GetLogger()}
			d := newSLDetector(0.99, l)
			forceCongestedWithDownwardTrend(t, d, c.ts, estimate)
			acked := d.congestedTrafficStats.AcknowledgedBitrate()
			require.Less(t, acked, estimate, "precondition: acked < estimate")

			notify, _, _, got, ev := d.congestionDetectionStateMachine()
			require.True(t, notify)
			require.Equal(t, acked, got)
			require.False(t, ev.SenderLimited)
			require.Equal(t, 1, l.count(slTrendLine))
		})
	}
}

// slQueuingGroups builds finalized packet groups of 30 packets / 150 ms each with a standing, slowly growing
// queuing delay (60 ms + 1 ms per group) and a captured traffic ratio of 150000/150100 per group.
func slQueuingGroups(n int) []*packetGroup {
	var groups []*packetGroup
	sn := uint64(1)
	sendTime := int64(1_000_000)
	for g := 0; g < n; g++ {
		pg := newPacketGroup(
			packetGroupParams{
				Config:       defaultPacketGroupConfig,
				WeightedLoss: defaultWeightedLossConfig,
				Logger:       logger.GetLogger(),
			},
			int64(60_000+g*1000),
		)
		for k := 0; k < defaultPacketGroupConfig.MinPackets; k++ {
			sendTime += 5000
			recvDelta := int64(5000)
			if k == 0 {
				recvDelta += 100
			}
			pi := &packetInfo{sequenceNumber: sn, sendTime: sendTime, recvTime: sendTime + 60_000, size: 1200}
			if err := pg.Add(pi, 5000, recvDelta, false); err != nil {
				panic(err)
			}
			sn++
		}
		groups = append(groups, pg)
	}
	return groups
}

// U4(a): the evidence is a value copy of the stats the estimate was made on. Mutant: evidence from the wrong stats.
func TestSenderLimitedEvidenceTransition(t *testing.T) {
	for _, c := range []struct {
		key  float64
		want bool
	}{
		{0.99, true},
		{0, false},
	} {
		d := newSLDetector(c.key, nil)
		d.congestionState = bwe.CongestionStateEarlyWarning
		d.packetGroups = slQueuingGroups(6)
		for _, pg := range d.packetGroups {
			require.True(t, pg.IsFinalized(), "precondition: finalized groups")
		}

		notify, from, to, estimate, ev := d.congestionDetectionStateMachine()
		require.True(t, notify)
		require.Equal(t, bwe.CongestionStateEarlyWarning, from)
		require.Equal(t, bwe.CongestionStateCongested, to)
		require.Equal(t, congestionReasonQueuingDelay, d.congestionReason)
		require.NotNil(t, d.estimateTrafficStats)
		require.Equal(t, d.estimateTrafficStats.AcknowledgedBitrate(), estimate)

		ctr := d.estimateTrafficStats.CapturedTrafficRatio()
		require.Greater(t, ctr, 0.99, "precondition: delay-only traffic")
		require.Less(t, ctr, 1.0, "precondition: CTR distinguishable from a fresh stats object")
		require.Equal(t, bwe.CongestionEvidence{
			SenderLimited:        c.want,
			CapturedTrafficRatio: ctr,
			WeightedLoss:         0,
		}, ev, "key %v", c.key)
	}
}

// a trend fire carries zero evidence: the allocator acts on transition evidence only (D-b7).
func TestSenderLimitedEvidenceTrendFire(t *testing.T) {
	d := newSLDetector(0.99, nil)
	ts := slTrafficStats(900, 1000, 0)
	forceCongestedWithDownwardTrend(t, d, ts, 2_000_000)

	notify, _, _, _, ev := d.congestionDetectionStateMachine()
	require.True(t, notify)
	require.Equal(t, bwe.CongestionEvidence{}, ev)
}

type slPlainListener struct {
	calls []int64
}

func (l *slPlainListener) OnCongestionStateChange(_, _ bwe.CongestionState, estimate int64) {
	l.calls = append(l.calls, estimate)
}

type slEvidenceListener struct {
	slPlainListener
	evidenceCalls []bwe.CongestionEvidence
}

func (l *slEvidenceListener) OnCongestionStateChangeWithEvidence(_, _ bwe.CongestionState, _ int64, ev bwe.CongestionEvidence) {
	l.evidenceCalls = append(l.evidenceCalls, ev)
}

// U4(b): the notify helper prefers the optional evidence interface and keeps the 3-arg call otherwise.
func TestSenderLimitedNotifyHelper(t *testing.T) {
	ev := bwe.CongestionEvidence{SenderLimited: true, CapturedTrafficRatio: 0.995}

	el := &slEvidenceListener{}
	notifyCongestionStateChange(el, bwe.CongestionStateNone, bwe.CongestionStateCongested, 123, ev)
	require.Equal(t, []bwe.CongestionEvidence{ev}, el.evidenceCalls)
	require.Empty(t, el.calls)

	pl := &slPlainListener{}
	notifyCongestionStateChange(pl, bwe.CongestionStateNone, bwe.CongestionStateCongested, 123, ev)
	require.Equal(t, []int64{123}, pl.calls)

	require.NotPanics(t, func() {
		notifyCongestionStateChange(nil, bwe.CongestionStateNone, bwe.CongestionStateCongested, 123, ev)
	})
}
