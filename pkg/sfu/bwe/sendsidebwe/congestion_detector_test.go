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
	"slices"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/mono"

	"github.com/livekit/livekit-server/pkg/sfu/bwe"
	"github.com/livekit/livekit-server/pkg/sfu/ccutils"
)

// the reserved symbol has no receive delta, so its packet is neither acked nor lost,
// and the next received packet uses the next delta
func TestHandleTWCCFeedbackReservedSymbol(t *testing.T) {
	// small delta, reserved, reserved, small delta, not received
	for name, chunks := range map[string][]rtcp.PacketStatusChunk{
		"status vector": {
			&rtcp.StatusVectorChunk{
				SymbolSize: rtcp.TypeTCCSymbolSizeTwoBit,
				SymbolList: []uint16{
					rtcp.TypeTCCPacketReceivedSmallDelta,
					rtcp.TypeTCCPacketReceivedWithoutDelta,
					rtcp.TypeTCCPacketReceivedWithoutDelta,
					rtcp.TypeTCCPacketReceivedSmallDelta,
					rtcp.TypeTCCPacketNotReceived,
					rtcp.TypeTCCPacketNotReceived,
					rtcp.TypeTCCPacketNotReceived,
				},
			},
		},
		"run length": {
			&rtcp.RunLengthChunk{PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 1},
			&rtcp.RunLengthChunk{PacketStatusSymbol: rtcp.TypeTCCPacketReceivedWithoutDelta, RunLength: 2},
			&rtcp.RunLengthChunk{PacketStatusSymbol: rtcp.TypeTCCPacketReceivedSmallDelta, RunLength: 1},
			&rtcp.RunLengthChunk{PacketStatusSymbol: rtcp.TypeTCCPacketNotReceived, RunLength: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCongestionDetector(congestionDetectorParams{
				Config: defaultCongestionDetectorConfig,
				Logger: logger.GetLogger(),
			})
			// the first packet sent is the send time base and never joins a group, so keep it out of the report
			now := mono.UnixMicro()
			c.RecordPacketSendAndGetSequenceNumber(now, 1200, false, ccutils.ProbeClusterIdInvalid, false)
			sns := make([]uint16, 5)
			for i := range sns {
				sns[i] = c.RecordPacketSendAndGetSequenceNumber(now+int64(i+1)*1000, 1200, false, ccutils.ProbeClusterIdInvalid, false)
			}

			fb := &rtcp.TransportLayerCC{
				Header: rtcp.Header{
					Count: rtcp.FormatTCC,
					Type:  rtcp.TypeTransportSpecificFeedback,
				},
				MediaSSRC:          1,
				BaseSequenceNumber: sns[0],
				PacketStatusCount:  uint16(len(sns)),
				ReferenceTime:      100,
				FbPktCount:         1,
				PacketChunks:       chunks,
				RecvDeltas: []*rtcp.RecvDelta{
					{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 1000},
					{Type: rtcp.TypeTCCPacketReceivedSmallDelta, Delta: 2000},
				},
			}
			fb.Header.Padding = fb.MarshalSize()%4 != 0
			fb.Header.Length = uint16(fb.MarshalSize()/4 - 1)

			// round trip so the report is exactly what a remote peer can deliver
			raw, err := fb.Marshal()
			require.NoError(t, err)
			pkts, err := rtcp.Unmarshal(raw)
			require.NoError(t, err)

			require.NotPanics(t, func() { c.HandleTWCCFeedback(pkts[0].(*rtcp.TransportLayerCC)) })

			// receive time is relative to the first received packet
			require.Equal(t, int64(2000), c.packetTracker.getPacketInfoExisting(sns[3]).recvTime)

			acked, lost := 0, 0
			for _, pg := range c.packetGroups {
				acked += pg.acked.numPackets()
				lost += pg.lost.numPackets()
			}
			require.Equal(t, 2, acked)
			require.Equal(t, 1, lost)
		})
	}
}

// -------------------------------------------------------------------------------
// queuing delay baseline window
//
// Simulated time only: send times are injected and receive times are encoded in
// TWCC reports (large delta symbols, round tripped through Marshal/Unmarshal),
// so a 30 s feed runs in milliseconds.

const (
	// a multiple of the TWCC receive delta resolution (250 us), so receive times round trip exactly;
	// ~308 pps, a 30 packet group (default MinPackets) spans 97.5 ms
	qdSimPacketSpacing    = 3250 * time.Microsecond
	qdSimPacketsPerGroup  = 30
	qdSimRecvOffsetMicro  = int64(1_000_000)
	qdSimTWCCResolution   = int64(250)
	qdSimRefTimeUnitMicro = int64(64_000)
)

func qdSimGroups(d time.Duration) int {
	return int(d / (qdSimPacketsPerGroup * qdSimPacketSpacing))
}

func flatOWD(d time.Duration) func(time.Duration) time.Duration {
	return func(time.Duration) time.Duration { return d }
}

func rampOWD(from time.Duration, perSecond time.Duration) func(time.Duration) time.Duration {
	return func(t time.Duration) time.Duration {
		return from + time.Duration(float64(perSecond)*t.Seconds())
	}
}

type qdSimPacket struct {
	sn     uint16
	recvAt int64 // micro seconds, 0 = lost
}

type qdSimTransition struct {
	at        time.Duration // simulated send time of the report's newest packet
	from      bwe.CongestionState
	to        bwe.CongestionState
	reason    congestionReason
	numGroups int
	estimate  int64
	pqds      []int64
}

type qdSimSample struct {
	state    bwe.CongestionState
	estimate int64
}

type qdSim struct {
	t       *testing.T
	c       *congestionDetector
	base    int64
	now     time.Duration
	fbCount uint8
	pending []qdSimPacket

	transitions []qdSimTransition
	samples     []qdSimSample
	onReport    func()
}

func newQDSim(t *testing.T, window time.Duration) *qdSim {
	cfg := defaultCongestionDetectorConfig
	cfg.QueuingDelayBaselineWindow = window
	return newQDSimWithConfig(t, cfg)
}

func newQDSimWithConfig(t *testing.T, cfg CongestionDetectorConfig) *qdSim {
	s := &qdSim{
		t: t,
		c: newCongestionDetector(congestionDetectorParams{
			Config: cfg,
			Logger: logger.GetLogger(),
		}),
		base: mono.UnixMicro(),
	}
	// the first packet sent is the send time base and never joins a group, so keep it out of the reports
	s.c.RecordPacketSendAndGetSequenceNumber(s.base, 1200, false, ccutils.ProbeClusterIdInvalid, false)
	return s
}

// run sends numGroups*qdSimPacketsPerGroup packets; the packet sent at t (since the start of this call)
// sees one-way delay owd(t); lossEvery > 0 drops every lossEvery-th packet
func (s *qdSim) run(numGroups int, owd func(time.Duration) time.Duration, lossEvery int) {
	s.runProbe(numGroups, owd, lossEvery, ccutils.ProbeClusterIdInvalid)
}

func (s *qdSim) runProbe(numGroups int, owd func(time.Duration) time.Duration, lossEvery int, probeClusterId ccutils.ProbeClusterId) {
	start := s.now
	for i := 0; i < numGroups*qdSimPacketsPerGroup; i++ {
		s.now += qdSimPacketSpacing
		sendAt := s.now.Microseconds()
		sn := s.c.RecordPacketSendAndGetSequenceNumber(s.base+sendAt, 1200, false, probeClusterId, probeClusterId != ccutils.ProbeClusterIdInvalid)
		p := qdSimPacket{sn: sn}
		if lossEvery <= 0 || (i+1)%lossEvery != 0 {
			d := owd(s.now - start).Microseconds()
			require.GreaterOrEqual(s.t, d, int64(0))
			p.recvAt = qdSimRecvOffsetMicro + sendAt + d - d%qdSimTWCCResolution
		}
		s.pending = append(s.pending, p)
		if len(s.pending) == qdSimPacketsPerGroup {
			s.flush()
		}
	}
}

func (s *qdSim) flush() {
	var chunks []rtcp.PacketStatusChunk
	var deltas []*rtcp.RecvDelta
	refTime := int64(-1)
	last := int64(0)
	for _, p := range s.pending {
		symbol := uint16(rtcp.TypeTCCPacketNotReceived)
		if p.recvAt != 0 {
			symbol = rtcp.TypeTCCPacketReceivedLargeDelta
			if refTime < 0 {
				refTime = p.recvAt / qdSimRefTimeUnitMicro
				last = refTime * qdSimRefTimeUnitMicro
			}
			deltas = append(deltas, &rtcp.RecvDelta{Type: rtcp.TypeTCCPacketReceivedLargeDelta, Delta: p.recvAt - last})
			last = p.recvAt
		}
		if n := len(chunks); n > 0 {
			if rl := chunks[n-1].(*rtcp.RunLengthChunk); rl.PacketStatusSymbol == symbol {
				rl.RunLength++
				continue
			}
		}
		chunks = append(chunks, &rtcp.RunLengthChunk{PacketStatusSymbol: symbol, RunLength: 1})
	}
	require.GreaterOrEqual(s.t, refTime, int64(0), "a report needs a received packet")

	s.fbCount++
	fb := &rtcp.TransportLayerCC{
		Header: rtcp.Header{
			Count: rtcp.FormatTCC,
			Type:  rtcp.TypeTransportSpecificFeedback,
		},
		MediaSSRC:          1,
		BaseSequenceNumber: s.pending[0].sn,
		PacketStatusCount:  uint16(len(s.pending)),
		ReferenceTime:      uint32(refTime),
		FbPktCount:         s.fbCount,
		PacketChunks:       chunks,
		RecvDeltas:         deltas,
	}
	fb.Header.Padding = fb.MarshalSize()%4 != 0
	fb.Header.Length = uint16(fb.MarshalSize()/4 - 1)
	raw, err := fb.Marshal()
	require.NoError(s.t, err)
	pkts, err := rtcp.Unmarshal(raw)
	require.NoError(s.t, err)
	s.pending = s.pending[:0]

	from := s.c.CongestionState()
	s.c.HandleTWCCFeedback(pkts[0].(*rtcp.TransportLayerCC))
	to := s.c.CongestionState()
	if to != from {
		tr := qdSimTransition{
			at:        s.now,
			from:      from,
			to:        to,
			reason:    s.c.congestionReason,
			numGroups: len(s.c.packetGroups),
			estimate:  s.c.estimatedAvailableChannelCapacity,
		}
		if s.c.qdMeasurement != nil {
			tr.pqds = slices.Clone(s.c.qdMeasurement.propagatedQueuingDelays)
		}
		s.transitions = append(s.transitions, tr)
	}
	s.samples = append(s.samples, qdSimSample{state: to, estimate: s.c.estimatedAvailableChannelCapacity})

	if s.onReport != nil {
		s.onReport()
	}
}

func (s *qdSim) firstTransition(from, to bwe.CongestionState) (qdSimTransition, bool) {
	for _, tr := range s.transitions {
		if tr.from == from && tr.to == to {
			return tr, true
		}
	}
	return qdSimTransition{}, false
}

func (s *qdSim) reached(to bwe.CongestionState) bool {
	for _, tr := range s.transitions {
		if tr.to == to {
			return true
		}
	}
	return false
}

// U1: a monotone ramp into a standing +66 ms one-way delay enters CONGESTED; the baseline window exits it
func TestQueuingDelayBaselineWindowExitsStandingDelayCongested(t *testing.T) {
	feed := func(t *testing.T, window time.Duration) (*qdSim, time.Duration) {
		s := newQDSim(t, window)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		for k := range 8 {
			s.run(1, flatOWD(time.Duration(52+2*k)*time.Millisecond), 0)
		}
		rampEnd := s.now
		s.run(qdSimGroups(30*time.Second), flatOWD(66*time.Millisecond), 0)
		return s, rampEnd
	}
	requirePreState := func(t *testing.T, s *qdSim, rampEnd time.Duration) {
		ew, ok := s.firstTransition(bwe.CongestionStateNone, bwe.CongestionStateEarlyWarning)
		require.True(t, ok, "precondition: EARLY_WARNING reached")
		congested, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
		require.True(t, ok, "precondition: CONGESTED reached")
		require.Less(t, ew.at, congested.at)
		require.LessOrEqual(t, congested.at, rampEnd+time.Second, "precondition: CONGESTED during or just after the ramp")
		require.Equal(t, congestionReasonQueuingDelay, congested.reason)
	}

	t.Run("window 10s exits", func(t *testing.T) {
		s, rampEnd := feed(t, 10*time.Second)
		requirePreState(t, s, rampEnd)

		exit, ok := s.firstTransition(bwe.CongestionStateCongested, bwe.CongestionStateNone)
		require.True(t, ok, "CONGESTED -> NONE expected with the baseline window")
		require.LessOrEqual(t, exit.at, rampEnd+10*time.Second+1500*time.Millisecond)
		require.Equal(t, bwe.CongestionStateNone, s.c.CongestionState())
		require.True(t, s.c.CanProbe())
	})

	t.Run("window 0 holds", func(t *testing.T) {
		s, rampEnd := feed(t, 0)
		requirePreState(t, s, rampEnd)

		_, ok := s.firstTransition(bwe.CongestionStateCongested, bwe.CongestionStateNone)
		require.False(t, ok)
		require.Equal(t, bwe.CongestionStateCongested, s.c.CongestionState())
	})
}

// U2: a short ramp into a standing +35 ms one-way delay (between DQR and JQR) holds EARLY_WARNING; the baseline window exits it
func TestQueuingDelayBaselineWindowExitsStandingDelayEarlyWarning(t *testing.T) {
	feed := func(t *testing.T, window time.Duration) (*qdSim, time.Duration) {
		s := newQDSim(t, window)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		for k := range 3 {
			s.run(1, flatOWD(time.Duration(52+2*k)*time.Millisecond), 0)
		}
		rampEnd := s.now
		s.run(qdSimGroups(30*time.Second), flatOWD(35*time.Millisecond), 0)
		return s, rampEnd
	}
	requirePreState := func(t *testing.T, s *qdSim) {
		_, ok := s.firstTransition(bwe.CongestionStateNone, bwe.CongestionStateEarlyWarning)
		require.True(t, ok, "precondition: EARLY_WARNING reached")
		require.False(t, s.reached(bwe.CongestionStateCongested), "precondition: CONGESTED never reached")
	}

	t.Run("window 10s exits", func(t *testing.T) {
		s, rampEnd := feed(t, 10*time.Second)
		requirePreState(t, s)

		exit, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateNone)
		require.True(t, ok, "EARLY_WARNING -> NONE expected with the baseline window")
		require.LessOrEqual(t, exit.at, rampEnd+10*time.Second+time.Second)
		require.Equal(t, bwe.CongestionStateNone, s.c.CongestionState())
	})

	t.Run("window 0 holds", func(t *testing.T) {
		s, _ := feed(t, 0)
		requirePreState(t, s)

		_, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateNone)
		require.False(t, ok)
		require.Equal(t, bwe.CongestionStateEarlyWarning, s.c.CongestionState())
	})
}

// U3: window 0 is identical to the key being absent, and never re-baselines
func TestQueuingDelayBaselineWindowOffIsUnchanged(t *testing.T) {
	parsed := defaultCongestionDetectorConfig
	require.NoError(t, yaml.Unmarshal([]byte("queuing_delay_baseline_window: 0s"), &parsed))
	require.Zero(t, parsed.QueuingDelayBaselineWindow)

	feed := func(t *testing.T, cfg CongestionDetectorConfig) *qdSim {
		s := newQDSimWithConfig(t, cfg)
		s.onReport = func() {
			for _, pg := range s.c.packetGroups {
				raw, rawOk := pg.FinalizedPropagatedQueuingDelay()
				eff, effOk := pg.FinalizedEffectiveQueuingDelay()
				require.Equal(t, rawOk, effOk)
				require.Equal(t, raw, eff, "baseline applied while off")
			}
		}
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		for k := range 8 {
			s.run(1, flatOWD(time.Duration(52+2*k)*time.Millisecond), 0)
		}
		s.run(qdSimGroups(15*time.Second), flatOWD(66*time.Millisecond), 0)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		return s
	}

	absent := feed(t, defaultCongestionDetectorConfig)
	off := feed(t, parsed)
	require.NotEmpty(t, absent.transitions, "precondition: the feed changes state")
	require.Equal(t, absent.transitions, off.transitions)
	require.Equal(t, absent.samples, off.samples)
}

// U4: a genuine ramp longer than the window is detected as early as without the window and stays CONGESTED
func TestQueuingDelayBaselineWindowKeepsGenuineRamp(t *testing.T) {
	feed := func(t *testing.T, window time.Duration) *qdSim {
		s := newQDSim(t, window)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		s.run(qdSimGroups(15*time.Second), rampOWD(0, 50*time.Millisecond), 0)
		return s
	}

	on := feed(t, 10*time.Second)
	off := feed(t, 0)
	onEntry, ok := on.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
	require.True(t, ok, "precondition: CONGESTED reached with the window")
	offEntry, ok := off.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
	require.True(t, ok, "precondition: CONGESTED reached without the window")
	require.InDelta(t, offEntry.numGroups, onEntry.numGroups, 1)
	require.Equal(t, bwe.CongestionStateCongested, on.c.CongestionState())
	require.Equal(t, bwe.CongestionStateCongested, off.c.CongestionState())
}

// U5: a slow genuine ramp (+10 ms/s for 20 s) stays CONGESTED past the window
func TestQueuingDelayBaselineWindowKeepsSlowRamp(t *testing.T) {
	s := newQDSim(t, 10*time.Second)
	s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
	s.run(qdSimGroups(20*time.Second), rampOWD(0, 10*time.Millisecond), 0)

	_, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
	require.True(t, ok, "precondition: CONGESTED reached")
	_, ok = s.firstTransition(bwe.CongestionStateCongested, bwe.CongestionStateNone)
	require.False(t, ok)
	require.Equal(t, bwe.CongestionStateCongested, s.c.CongestionState())
}

// U6: loss driven congestion is not touched by the window
func TestQueuingDelayBaselineWindowKeepsLossCongestion(t *testing.T) {
	s := newQDSim(t, 10*time.Second)
	s.run(qdSimGroups(20*time.Second), flatOWD(0), 7)

	congested, ok := s.firstTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
	require.True(t, ok, "precondition: CONGESTED reached")
	require.Equal(t, congestionReasonLoss, congested.reason)
	require.Less(t, congested.at, 10*time.Second, "precondition: CONGESTED entered before the window elapsed")
	require.Greater(t, s.now, congested.at+10*time.Second)

	_, ok = s.firstTransition(bwe.CongestionStateCongested, bwe.CongestionStateNone)
	require.False(t, ok)
	require.Equal(t, bwe.CongestionStateCongested, s.c.CongestionState())
	require.Equal(t, congestionReasonLoss, s.c.congestionReason)
}

// U7: 0 <= effective <= raw on every finalized group, through a step up and back down
func TestQueuingDelayBaselineWindowEffectiveBounds(t *testing.T) {
	s := newQDSim(t, 10*time.Second)
	numChecked := 0
	s.onReport = func() {
		for _, pg := range s.c.packetGroups {
			raw, rawOk := pg.FinalizedPropagatedQueuingDelay()
			eff, effOk := pg.FinalizedEffectiveQueuingDelay()
			require.Equal(t, rawOk, effOk)
			if !effOk {
				continue
			}
			require.GreaterOrEqual(t, eff, int64(0))
			require.LessOrEqual(t, eff, raw)
			numChecked++
		}
	}
	s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
	s.run(qdSimGroups(30*time.Second), flatOWD(60*time.Millisecond), 0)
	drop := s.now
	s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)

	require.NotZero(t, numChecked)
	for _, tr := range s.transitions {
		require.LessOrEqual(t, tr.at, drop, "no state change after the drop")
	}
	require.Equal(t, bwe.CongestionStateNone, s.c.CongestionState())
}

// U8: probe verdicts are anchored per probe, the window does not leak into them
func TestQueuingDelayBaselineWindowProbeUnchanged(t *testing.T) {
	probe := func(t *testing.T, window time.Duration) (ccutils.ProbeSignal, int64, int64) {
		s := newQDSim(t, window)
		s.run(qdSimGroups(5*time.Second), flatOWD(0), 0)
		s.run(qdSimGroups(15*time.Second), flatOWD(60*time.Millisecond), 0)
		require.Empty(t, s.transitions, "precondition: a standing step does not leave NONE")

		pci := ccutils.ProbeClusterInfo{
			Id:        1,
			CreatedAt: mono.Now(),
			Goal: ccutils.ProbeClusterGoal{
				DesiredBps:   1_000_000,
				Duration:     200 * time.Millisecond,
				DesiredBytes: 25_000,
			},
		}
		s.c.ProbeClusterStarting(pci)
		s.runProbe(2, flatOWD(60*time.Millisecond), 0, pci.Id)
		require.NotNil(t, s.c.probePacketGroup)
		pqd := s.c.probePacketGroup.PropagatedQueuingDelay()

		s.c.ProbeClusterDone(pci)
		signal, estimate, isFinalized := s.c.ProbeClusterFinalize()
		require.True(t, isFinalized)
		return signal, pqd, estimate
	}

	offSignal, offPQD, offEstimate := probe(t, 0)
	onSignal, onPQD, onEstimate := probe(t, 10*time.Second)
	require.Equal(t, offSignal, onSignal)
	require.Equal(t, offPQD, onPQD)
	require.Equal(t, offEstimate, onEstimate)
}
