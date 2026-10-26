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

package streamallocator

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/bwe"
)

const (
	slCommitLine = "stream allocator: sender-limited congestion, committing expected usage"
	// logged by getAvailableChannelCapacity(true) when an override is set; allocateAllTracks is its only caller on the
	// congestion path, so with overriddenChannelCapacity set to the budget the clamp would commit, it is a spy on
	// allocateAllTracks that does not change the budget.
	slSpyLine = "stream allocator: overriding channel capacity"
)

// floorCongestedBWE reports CONGESTED, so allocateTrack takes the deficient (cooperative) path.
type floorCongestedBWE struct {
	floorNullBWE
}

func (floorCongestedBWE) CongestionState() bwe.CongestionState {
	return bwe.CongestionStateCongested
}

type slLogger struct {
	logger.Logger

	mu     sync.Mutex
	infos  []string
	debugs []string
}

func (l *slLogger) Infow(msg string, keysAndValues ...any) {
	l.mu.Lock()
	l.infos = append(l.infos, msg)
	l.mu.Unlock()
	l.Logger.Infow(msg, keysAndValues...)
}

func (l *slLogger) Debugw(msg string, keysAndValues ...any) {
	l.mu.Lock()
	l.debugs = append(l.debugs, msg)
	l.mu.Unlock()
	l.Logger.Debugw(msg, keysAndValues...)
}

func slCount(msgs []string, prefix string) int {
	n := 0
	for _, msg := range msgs {
		if strings.HasPrefix(msg, prefix) {
			n++
		}
	}
	return n
}

func (l *slLogger) infoCount(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slCount(l.infos, prefix)
}

func (l *slLogger) debugCount(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slCount(l.debugs, prefix)
}

func newSLFixture(t *testing.T) (*floorFixture, *slLogger) {
	l := &slLogger{Logger: logger.GetLogger()}
	return newFloorFixture(t, 1, l), l
}

// addWithMax adds a camera with the given max layers.
func (c *floorFixture) addWithMax(id string, maxSpatial, maxTemporal int32) *Track {
	c.t.Helper()

	track := c.addTrack(id, livekit.TrackSource_CAMERA, floorBrsLadder)
	track.downTrack.SetMaxSpatialLayer(maxSpatial)
	track.downTrack.SetMaxTemporalLayer(maxTemporal)
	return track
}

// allocateAll targets every track at its max layer with an ample budget (the free pass would overshoot to the
// max seen layer on a track that has not forwarded yet).
func (c *floorFixture) allocateAll() {
	c.s.committedChannelCapacity = 100_000
	c.s.allocateAllTracks()
	require.Equal(c.t, streamAllocatorStateStable, c.s.state, "precondition: stable")
}

func (c *floorFixture) addAllocated(id string, maxSpatial, maxTemporal int32) *Track {
	c.t.Helper()

	track := c.addWithMax(id, maxSpatial, maxTemporal)
	c.allocateAll()
	return track
}

func (c *floorFixture) notify(from, to bwe.CongestionState, estimate int64, senderLimited bool) {
	c.notifyEvidence(from, to, estimate, bwe.CongestionEvidence{SenderLimited: senderLimited, CapturedTrafficRatio: 0.998})
}

func (c *floorFixture) notifyEvidence(from, to bwe.CongestionState, estimate int64, evidence bwe.CongestionEvidence) {
	c.s.handleSignalCongestionStateChange(Event{
		congestionStateChangeData: congestionStateChangeData{
			fromState:                         from,
			toState:                           to,
			estimatedAvailableChannelCapacity: estimate,
			evidence:                          evidence,
		},
	})
}

// congestedSL notifies the only transition into CONGESTED the send side detector makes: EARLY_WARNING -> CONGESTED.
func (c *floorFixture) congestedSL(estimate int64, senderLimited bool) {
	c.notify(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested, estimate, senderLimited)
}

var (
	slS0T1 = buffer.VideoLayer{Spatial: 0, Temporal: 1}
	slS0T2 = buffer.VideoLayer{Spatial: 0, Temporal: 2}
	slS2T1 = buffer.VideoLayer{Spatial: 2, Temporal: 1}
	slS2T2 = buffer.VideoLayer{Spatial: 2, Temporal: 2}
)

// U5: A-2 clamp. Mutants: clamp to the estimate; allocateAllTracks still called on the clamp.
func TestSenderLimitedClamp(t *testing.T) {
	t.Run("PRE stock: not sender limited", func(t *testing.T) {
		fx, l := newSLFixture(t)
		track := fx.addAllocated("a", 0, 2)
		require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder), "precondition: S0T2")
		usage := fx.s.getExpectedBandwidthUsage()

		fx.congestedSL(usage-40, false)
		require.Equal(t, usage-40, fx.s.committedChannelCapacity)
		require.Equal(t, slS0T1, floorTarget(t, track, floorBrsLadder), "the fixture reproduces R1: one temporal step shed")
		require.Equal(t, 0, l.infoCount(slCommitLine))
	})

	t.Run("POST sender limited", func(t *testing.T) {
		fx, l := newSLFixture(t)
		track := fx.addAllocated("a", 0, 2)
		require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder), "precondition: S0T2")
		usage := fx.s.getExpectedBandwidthUsage()
		fx.s.overriddenChannelCapacity = usage

		fx.congestedSL(usage-40, true)
		require.Equal(t, usage, fx.s.committedChannelCapacity)
		require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder))
		require.Equal(t, streamAllocatorStateStable, fx.s.state)
		require.Equal(t, 1, l.infoCount(slCommitLine))
		require.Equal(t, 0, l.debugCount(slSpyLine), "allocateAllTracks must not run on a clamp")
	})
}

// U6: A-2 never raises the budget above the estimate. Mutant: committed = usage unconditionally.
func TestSenderLimitedNoClampAboveUsage(t *testing.T) {
	fx, l := newSLFixture(t)
	track := fx.addAllocated("a", 0, 2)
	usage := fx.s.getExpectedBandwidthUsage()
	estimate := usage + 100
	fx.s.overriddenChannelCapacity = estimate

	fx.congestedSL(estimate, true)
	require.Equal(t, estimate, fx.s.committedChannelCapacity)
	require.Equal(t, 1, l.debugCount(slSpyLine), "stock path: allocateAllTracks runs")
	require.Equal(t, 0, l.infoCount(slCommitLine))
	require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder))
}

// heldPair: A streams S2T1 at its max; B streams S0T2 and is then allowed S2, but a cooperative allocation while
// CONGESTED keeps its target. A stock pass at the same budget would move bits from A to B.
func heldPair(t *testing.T, fx *floorFixture) (*Track, *Track) {
	t.Helper()

	a := fx.addWithMax("a", 2, 1)
	b := fx.addWithMax("b", 0, 2)
	fx.allocateAll()
	fx.s.params.BWE = floorCongestedBWE{floorNullBWE{NullBWE: &bwe.NullBWE{}}}
	b.downTrack.SetMaxSpatialLayer(2)
	fx.s.allocateTrack(b)
	require.Equal(t, slS2T1, floorTarget(t, a, floorBrsLadder), "precondition: A S2T1")
	require.Equal(t, slS0T2, floorTarget(t, b, floorBrsLadder), "precondition: B held at S0T2")
	require.Equal(t, streamAllocatorStateStable, fx.s.state, "precondition: stable")
	return a, b
}

// U7: A-2 multi-track: a clamp moves no track. Mutant: allocateAllTracks still called on the clamp.
func TestSenderLimitedClampMultiTrack(t *testing.T) {
	t.Run("PRE stock pass at the same budget reshuffles", func(t *testing.T) {
		fx, _ := newSLFixture(t)
		a, b := heldPair(t, fx)
		usage := fx.s.getExpectedBandwidthUsage()

		fx.congestedSL(usage-1, false)
		changed := floorTarget(t, a, floorBrsLadder) != slS2T1 || floorTarget(t, b, floorBrsLadder) != slS0T2
		require.True(t, changed, "precondition: a stock pass at ~usage moves a track")
	})

	t.Run("POST clamp", func(t *testing.T) {
		fx, l := newSLFixture(t)
		a, b := heldPair(t, fx)
		usage := fx.s.getExpectedBandwidthUsage()
		fx.s.overriddenChannelCapacity = usage

		fx.congestedSL(usage-1, true)
		require.Equal(t, usage, fx.s.committedChannelCapacity)
		require.Equal(t, slS2T1, floorTarget(t, a, floorBrsLadder))
		require.Equal(t, slS0T2, floorTarget(t, b, floorBrsLadder))
		require.Equal(t, 0, l.debugCount(slSpyLine))
	})
}

// U7b: after a clamp, a newcomer finds no headroom and must take the steal path. Mutant: the clamp commits
// usage + delta with delta >= the newcomer's lowest layer bitrate (floorBrsLadder S0T0 = 100), which funds the
// newcomer from headroom.
func TestSenderLimitedClampThenNewcomer(t *testing.T) {
	fx, _ := newSLFixture(t)
	a, b := heldPair(t, fx)
	usage := fx.s.getExpectedBandwidthUsage()
	fx.congestedSL(usage-1, true)
	require.Equal(t, usage, fx.s.committedChannelCapacity, "precondition: clamped at usage")

	preA, preB := floorTarget(t, a, floorBrsLadder), floorTarget(t, b, floorBrsLadder)
	preExisting := a.BandwidthRequested() + b.BandwidthRequested()

	c := fx.addTrack("c", livekit.TrackSource_CAMERA, floorBrsLadder)
	c.downTrack.SetMaxTemporalLayer(2)
	c.Pause()
	require.Zero(t, c.BandwidthRequested(), "precondition: newcomer not streaming")
	require.Equal(t, fx.s.committedChannelCapacity, fx.s.getExpectedBandwidthUsage(), "precondition: no headroom")

	fx.s.allocateTrack(c)

	require.False(t, floorTarget(t, a, floorBrsLadder).GreaterThan(preA), "A above its pre-clamp target")
	require.False(t, floorTarget(t, b, floorBrsLadder).GreaterThan(preB), "B above its pre-clamp target")

	gaveUp := a.BandwidthRequested()+b.BandwidthRequested() < preExisting
	require.True(t, gaveUp || c.BandwidthRequested() == 0, "newcomer funded only by bits the others gave up")
}

// sender-limited CONGESTED, then the viewport grows: the cooperative path keeps S0 while CONGESTED.
func slHeldAtS0(t *testing.T, fx *floorFixture, estimate int64, senderLimited bool) *Track {
	t.Helper()

	track := fx.addAllocated("a", 0, 2)
	fx.s.params.BWE = floorCongestedBWE{floorNullBWE{NullBWE: &bwe.NullBWE{}}}
	fx.congestedSL(estimate, senderLimited)
	return track
}

func slGrowViewport(t *testing.T, fx *floorFixture, track *Track) {
	t.Helper()

	track.downTrack.SetMaxSpatialLayer(2)
	fx.s.allocateTrack(track)
	require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder), "precondition: no up-move while CONGESTED")
	require.Equal(t, streamAllocatorStateStable, fx.s.state, "precondition: stable")
}

// U8: A-3 release at CONGESTED -> NONE. Mutants: release ungated; release on EARLY_WARNING only.
func TestSenderLimitedReleaseAtNone(t *testing.T) {
	t.Run("sender limited: released", func(t *testing.T) {
		fx, _ := newSLFixture(t)
		track := slHeldAtS0(t, fx, 160, true)
		require.Equal(t, int64(200), fx.s.committedChannelCapacity, "precondition: clamped")
		slGrowViewport(t, fx, track)

		fx.notify(bwe.CongestionStateCongested, bwe.CongestionStateNone, 160, false)
		require.Equal(t, slS2T2, floorTarget(t, track, floorBrsLadder))
	})

	t.Run("not sender limited: stock, held", func(t *testing.T) {
		fx, _ := newSLFixture(t)
		track := slHeldAtS0(t, fx, 200, false)
		slGrowViewport(t, fx, track)

		fx.notify(bwe.CongestionStateCongested, bwe.CongestionStateNone, 200, false)
		require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder))
	})
}

// U8b: the flag does not survive a STABLE entry (BWE reset, no NONE notify). Mutant: flag not cleared in setState.
func TestSenderLimitedFlagClearedOnStable(t *testing.T) {
	fx, _ := newSLFixture(t)
	track := slHeldAtS0(t, fx, 160, true)
	slGrowViewport(t, fx, track)

	fx.s.setState(streamAllocatorStateDeficient)
	fx.s.setState(streamAllocatorStateStable)

	fx.notify(bwe.CongestionStateCongested, bwe.CongestionStateNone, 160, false)
	require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder))
}

// U8c: the flag is the latest TRANSITION evidence (D-b10): a zero-evidence trend fire in the same episode keeps it,
// and the release happens at NONE. Mutant: the fromState guard removed (the trend fire clears the flag).
func TestSenderLimitedFlagLatestWins(t *testing.T) {
	fx, _ := newSLFixture(t)
	track := slHeldAtS0(t, fx, 160, true)
	require.Equal(t, int64(200), fx.s.committedChannelCapacity, "precondition: clamped")
	require.True(t, fx.s.senderLimitedCongestion, "precondition: flag set by the transition")

	// trend re-commit in the same episode, zero evidence, at usage: the stock pass keeps S0T2
	fx.notifyEvidence(bwe.CongestionStateCongested, bwe.CongestionStateCongested, 200, bwe.CongestionEvidence{})
	require.True(t, fx.s.senderLimitedCongestion, "a trend fire keeps the transition flag")
	slGrowViewport(t, fx, track)

	fx.notify(bwe.CongestionStateCongested, bwe.CongestionStateNone, 200, false)
	require.Equal(t, slS2T2, floorTarget(t, track, floorBrsLadder))
}

// U8d: a new transition overwrites the flag. Reachable when the BWE resets to NONE without a notify (periodic ping
// while DEFICIENT) and a new episode starts. Mutant: the flag sticks (`flag || evidence.SenderLimited`).
func TestSenderLimitedFlagLatestTransitionWins(t *testing.T) {
	fx, _ := newSLFixture(t)
	track := slHeldAtS0(t, fx, 160, true)
	require.True(t, fx.s.senderLimitedCongestion, "precondition: flag set by the transition")

	fx.congestedSL(200, false)
	require.False(t, fx.s.senderLimitedCongestion, "the latest transition evidence wins")
	slGrowViewport(t, fx, track)

	fx.notify(bwe.CongestionStateCongested, bwe.CongestionStateNone, 200, false)
	require.Equal(t, slS0T2, floorTarget(t, track, floorBrsLadder))
}

// U-alloc-trend: after a sender-limited transition clamp, a zero-evidence trend fire below usage takes the stock
// path (commit the estimate, re-allocate) and keeps the flag. Mutants: A-2 reads the flag instead of the notify's
// evidence (clamps again); the fromState guard removed (flag cleared).
func TestSenderLimitedTrendFireAfterClamp(t *testing.T) {
	fx, l := newSLFixture(t)
	fx.addAllocated("a", 0, 2)
	usage := fx.s.getExpectedBandwidthUsage()
	fx.s.overriddenChannelCapacity = usage

	fx.congestedSL(usage-40, true)
	require.Equal(t, usage, fx.s.committedChannelCapacity, "precondition: clamped at usage")
	require.True(t, fx.s.senderLimitedCongestion, "precondition: flag set")
	require.Equal(t, 1, l.infoCount(slCommitLine), "precondition: A-2 fired")
	require.Equal(t, 0, l.debugCount(slSpyLine), "precondition: no re-allocation on the clamp")

	fx.notifyEvidence(bwe.CongestionStateCongested, bwe.CongestionStateCongested, usage-40, bwe.CongestionEvidence{})
	require.Equal(t, usage-40, fx.s.committedChannelCapacity)
	require.Equal(t, 1, l.debugCount(slSpyLine), "stock path: allocateAllTracks runs")
	require.Equal(t, 1, l.infoCount(slCommitLine), "no second clamp")
	require.True(t, fx.s.senderLimitedCongestion, "flag unchanged by a trend fire")
}

// U9: the 3-arg listener path (RemoteBWE, old producers) is stock: zero evidence, so the estimate is committed and
// the release flag stays false.
func TestSenderLimitedThreeArgPath(t *testing.T) {
	fx, l := newSLFixture(t)
	track := fx.addAllocated("a", 0, 2)
	usage := fx.s.getExpectedBandwidthUsage()

	fx.s.eventsQueue.Start()
	t.Cleanup(func() { fx.s.eventsQueue.Stop() })

	fx.s.OnCongestionStateChange(bwe.CongestionStateNone, bwe.CongestionStateCongested, usage-40)

	type observed struct {
		committed int64
		target    int64
	}
	done := make(chan observed)
	fx.s.eventsQueue.Enqueue(func(Event) {
		done <- observed{fx.s.committedChannelCapacity, track.BandwidthRequested()}
	}, Event{})
	got := <-done

	require.Equal(t, usage-40, got.committed)
	require.Equal(t, floorBrsLadder[0][1], got.target, "stock: one temporal step shed")
	require.Equal(t, 0, l.infoCount(slCommitLine))
}
