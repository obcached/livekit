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
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// Forward stall: a video DownTrack that is offered packets (WriteRTP is called past the
// writable gate) but forwards none of them, while it is bound, unmuted and has a valid
// target layer. The check is observation only: it logs, it never changes forwarding.
//
// All times are mono.UnixNano() values: the caller's clock, packet arrivals and the
// bind time share that domain.

const (
	forwardStallOnset          = 5 * time.Second
	forwardStallRepeat         = 30 * time.Second
	forwardStallMaxLines       = 10 // onset + repeat lines per DownTrack
	forwardStallLinesPerSecond = 10 // process wide
)

type forwardStallEvent int

const (
	forwardStallNone forwardStallEvent = iota
	forwardStallOnsetLine
	forwardStallRepeatLine
	forwardStallEnded
)

type forwardStallSample struct {
	// every gate except "offered advanced" holds
	eligible  bool
	offered   uint64
	dropped   uint64
	forwarded uint64
}

type forwardStallResult struct {
	event          forwardStallEvent
	stallNanos     int64
	offeredDelta   uint64
	droppedDelta   uint64
	forwardedDelta uint64
}

type forwardStallState struct {
	mu sync.Mutex

	started       bool
	prevOffered   uint64
	prevForwarded uint64

	// time of the last check that was not a stalled check (bind time initially),
	// with the counters at that check
	stallStart     int64
	startOffered   uint64
	startDropped   uint64
	startForwarded uint64

	logged     bool // an onset line was emitted for the current stall
	lastLineAt int64
	lines      int
}

// step classifies one check. A stalled check is one where forwarded did not advance while
// offered did, since the previous check, with every gate holding.
func (s *forwardStallState) step(sm forwardStallSample, nowMono int64, boundAt int64) forwardStallResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		s.started = true
		s.stallStart = nowMono
		if boundAt != 0 && boundAt <= nowMono {
			s.stallStart = boundAt
		}
	}

	stalled := sm.eligible && sm.forwarded == s.prevForwarded && sm.offered > s.prevOffered
	s.prevOffered = sm.offered
	s.prevForwarded = sm.forwarded

	r := forwardStallResult{
		stallNanos:     nowMono - s.stallStart,
		offeredDelta:   sm.offered - s.startOffered,
		droppedDelta:   sm.dropped - s.startDropped,
		forwardedDelta: sm.forwarded - s.startForwarded,
	}

	if !stalled {
		if s.logged {
			s.logged = false
			r.event = forwardStallEnded
		}
		s.stallStart = nowMono
		s.startOffered = sm.offered
		s.startDropped = sm.dropped
		s.startForwarded = sm.forwarded
		return r
	}

	if r.stallNanos <= int64(forwardStallOnset) || s.lines >= forwardStallMaxLines {
		return r
	}
	switch {
	case !s.logged:
		s.logged = true
		r.event = forwardStallOnsetLine
	case nowMono-s.lastLineAt >= int64(forwardStallRepeat):
		r.event = forwardStallRepeatLine
	default:
		return r
	}
	s.lines++
	s.lastLineAt = nowMono
	return r
}

// -------------------------------------------------------------------

// forwardStallLimiter is a token bucket shared by every DownTrack of the process.
type forwardStallLimiter struct {
	mu      sync.Mutex
	tokens  float64
	last    int64
	dropped uint64
}

func newForwardStallLimiter() *forwardStallLimiter {
	return &forwardStallLimiter{tokens: forwardStallLinesPerSecond}
}

var forwardStallLogLimiter = newForwardStallLimiter()

// allow takes a token; it returns the cumulative count of lines dropped.
func (l *forwardStallLimiter) allow(nowMono int64) (bool, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.last != 0 && nowMono > l.last {
		l.tokens += float64(nowMono-l.last) / float64(time.Second) * forwardStallLinesPerSecond
		if l.tokens > forwardStallLinesPerSecond {
			l.tokens = forwardStallLinesPerSecond
		}
	}
	l.last = nowMono

	if l.tokens < 1 {
		l.dropped++
		return false, l.dropped
	}
	l.tokens--
	return true, l.dropped
}

// -------------------------------------------------------------------

// SwitchRejectReason is why the forwarder last refused to switch to a new source.
type SwitchRejectReason uint8

const (
	SwitchRejectNone SwitchRejectReason = iota
	SwitchRejectSkipStartOutOfOrder
	SwitchRejectRefTSUnavailable
	SwitchRejectTooFarBehind
	SwitchRejectOther
)

func (r SwitchRejectReason) String() string {
	switch r {
	case SwitchRejectNone:
		return "none"
	case SwitchRejectSkipStartOutOfOrder:
		return "skip_start_ooo"
	case SwitchRejectRefTSUnavailable:
		return "ref_ts_unavailable"
	case SwitchRejectTooFarBehind:
		return "too_far_behind"
	}
	return "other"
}

// switchRejectReasonOf maps a processSourceSwitch error, comparing sentinels only.
// processSourceSwitch returns either a sentinel or the getRefLayerRTPTimestamp error.
func switchRejectReasonOf(err error) SwitchRejectReason {
	switch err {
	case nil:
		return SwitchRejectOther
	case errSkipStartOnOutOfOrderPacket:
		return SwitchRejectSkipStartOutOfOrder
	case errSwitchPointTooFarBehind:
		return SwitchRejectTooFarBehind
	}
	return SwitchRejectRefTSUnavailable
}

type ForwarderSwitchDebug struct {
	LastSSRC                uint32
	LastSwitchExtIncomingTS uint64
	LastReject              SwitchRejectReason
	Rejects                 uint32
	LastRejectAt            int64 // mono nanos, 0 = never
}

func (f *Forwarder) SwitchDebug() ForwarderSwitchDebug {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return ForwarderSwitchDebug{
		LastSSRC:                f.lastSSRC,
		LastSwitchExtIncomingTS: f.lastSwitchExtIncomingTS,
		LastReject:              f.lastSwitchReject,
		Rejects:                 f.switchRejects,
		LastRejectAt:            f.lastSwitchRejectAt,
	}
}

func agoMs(nowMono int64, at int64) int64 {
	if at == 0 {
		return -1
	}
	return (nowMono - at) / int64(time.Millisecond)
}

// appendForwarderStallFields adds the forwarder's timestamp state, read only when a line
// is emitted (GetState allocates).
func appendForwarderStallFields(kv []any, f *Forwarder, nowMono int64) []any {
	if st := f.GetState(); st != nil {
		srLayers := make([]string, 0, len(st.SenderReportState))
		for layer, sr := range st.SenderReportState {
			if sr == nil {
				continue
			}
			srLayers = append(srLayers, fmt.Sprintf("%d:%d/%d", layer, sr.RtpTimestampExt, sr.NtpTimestamp))
		}
		kv = append(kv,
			"dummyStartTSOffset", int64(st.DummyStartTimestampOffset),
			"referenceLayerSpatial", st.ReferenceLayerSpatial,
			"extFirstTS", st.ExtFirstTimestamp,
			"mungerLastExtSN", st.RtpMunger.GetExtLastSequenceNumber(),
			"mungerLastExtTS", st.RtpMunger.GetExtLastTimestamp(),
			"srLayers", srLayers,
		)
	} else {
		kv = append(kv, "forwarderStarted", false)
	}

	sd := f.SwitchDebug()
	return append(kv,
		"lastSSRC", sd.LastSSRC,
		"lastSwitchExtIncomingTS", sd.LastSwitchExtIncomingTS,
		"lastSwitchReject", sd.LastReject.String(),
		"switchRejects", sd.Rejects,
		"lastSwitchRejectAgoMs", agoMs(nowMono, sd.LastRejectAt),
	)
}

// -------------------------------------------------------------------

// CheckForwardStall is called periodically (by the subscriber's RTCP worker) with
// nowMono = mono.UnixNano(), and logs a video DownTrack that is offered packets but
// forwards none for more than forwardStallOnset.
func (d *DownTrack) CheckForwardStall(nowMono int64) {
	if d.kind != webrtc.RTPCodecTypeVideo {
		return
	}

	closed := d.isClosed.Load()
	sm := forwardStallSample{
		offered:   d.fwdOffered.Load(),
		dropped:   d.fwdDropped.Load(),
		forwarded: d.rtpStats.GetPacketsSeenMinusPadding(),
	}
	sm.eligible = !closed &&
		d.writable.Load() &&
		d.bindState.Load() == bindStateBound &&
		!d.forwarder.IsMuted() &&
		!d.forwarder.IsPubMuted() &&
		d.forwarder.TargetLayer().IsValid()

	r := d.forwardStall.step(sm, nowMono, d.boundAt.Load())
	if r.event == forwardStallNone {
		return
	}
	ok, droppedLines := forwardStallLogLimiter.allow(nowMono)
	if !ok {
		return
	}

	if r.event == forwardStallEnded {
		d.params.Logger.Infow(
			"downtrack forward stall ended",
			"stallMs", r.stallNanos/int64(time.Millisecond),
			"closed", closed,
			"droppedLines", droppedLines,
		)
		return
	}

	line := "onset"
	if r.event == forwardStallRepeatLine {
		line = "repeat"
	}
	lastKeyFrameFwdAgoMs := int64(-1)
	if _, at := d.rtpStats.KeyFrame(); !at.IsZero() {
		// wall clock, as recorded by rtpStats
		lastKeyFrameFwdAgoMs = time.Since(at).Milliseconds()
	}
	syncLocked, syncLayer := d.forwarder.CheckSync()
	kv := []any{
		"line", line,
		"stallMs", r.stallNanos / int64(time.Millisecond),
		"offeredDelta", r.offeredDelta,
		"droppedDelta", r.droppedDelta,
		"forwardedDelta", r.forwardedDelta,
		"lastKeyFrameFwdAgoMs", lastKeyFrameFwdAgoMs,
		"lastPliAgoMs", agoMs(nowMono, d.rtpStats.LastPli()),
		"targetLayer", d.forwarder.TargetLayer(),
		"currentLayer", d.forwarder.CurrentLayer(),
		"maxLayer", d.forwarder.MaxLayer(),
		"isDeficient", d.forwarder.IsDeficient(),
		"syncLocked", syncLocked,
		"syncLayer", syncLayer,
		"keyFrameRequesterWakeups", d.keyFrameRequesterWakeups.Load(),
		"mime", d.Mime(),
		"bindState", d.bindState.Load(),
		"rtt", d.rtpStats.GetRtt(),
		"fault", d.zeroForwardFault != nil,
		"droppedLines", droppedLines,
	}
	d.params.Logger.Infow("downtrack forward stall", appendForwarderStallFields(kv, d.forwarder, nowMono)...)
}
