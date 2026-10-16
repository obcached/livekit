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
	"time"

	"github.com/livekit/protocol/utils/mono"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

// Test-only seams that expose DownTrack internals to the external sfu_test package.
// This file is only compiled into the test binary (its name ends in _test.go), so it
// does not affect the production API. The external test package is required because the
// integration tests import the streamallocator package, which itself imports sfu (an
// import cycle that an in-package test cannot resolve).

func (d *DownTrack) IsWritableForTest() bool { return d.writable.Load() }

func (d *DownTrack) PayloadTypeRTXForTest() uint32 { return d.payloadTypeRTX.Load() }

func (d *DownTrack) RTPStatsActiveForTest() bool { return d.rtpStats.IsActive() }

const DroppedSlotPaddingSizeForTest = droppedSlotPaddingSize

func (d *DownTrack) WriteDroppedSlotPaddingForTest(extSequenceNumber uint64, extTimestamp uint64) {
	d.writeDroppedSlotPadding(extSequenceNumber, extTimestamp, false)
}

func (d *DownTrack) LastReceiverReportTimeForTest() int64 {
	return d.rtpStats.LastReceiverReportTime()
}

func (d *DownTrack) ExtIDsForTest() (absSendTime int, transportWide int) {
	return d.absSendTimeExtID, d.transportWideExtID
}

func (d *DownTrack) HandleRTCPForTest(pkt []byte) { d.handleRTCP(pkt) }

func (d *DownTrack) SetRTPHeaderExtensionsForTest() { d.setRTPHeaderExtensions() }

// ForceForwardLayerForTest forces the forwarder onto a valid current/target layer so a
// (non-simulcast) video packet will be forwarded without driving full allocation.
func (d *DownTrack) ForceForwardLayerForTest(layer buffer.VideoLayer) {
	d.forwarder.ForceLayerForTest(layer)
}

// RetransmitForTest drives the (unexported) retransmitPacket path with an in-package
// extPacketMeta built from exported values, so the external test can exercise the
// retransmit path.
func (d *DownTrack) RetransmitForTest(
	sourceSeqNo uint64,
	targetSeqNo uint16,
	timestamp uint32,
	extTimestamp uint64,
	layer int8,
	sourcePkt []byte,
	trailerStripped uint8,
) (int, error) {
	epm := extPacketMeta{
		packetMeta: packetMeta{
			sourceSeqNo:     sourceSeqNo,
			targetSeqNo:     targetSeqNo,
			timestamp:       timestamp,
			layer:           layer,
			trailerStripped: trailerStripped,
		},
		extSequenceNumber: uint64(targetSeqNo),
		extTimestamp:      extTimestamp,
	}
	return d.retransmitPacket(&epm, sourcePkt, false)
}

const KeyFrameIntervalMaxForTest = keyFrameIntervalMax * time.Millisecond

func (d *DownTrack) KeyFrameRequesterWakeupsForTest() uint32 {
	return d.keyFrameRequesterWakeups.Load()
}

// RetransmitPacketsForTest runs the NACK retransmit path synchronously (no worker).
func (d *DownTrack) RetransmitPacketsForTest(nacks []uint16) { d.retransmitPackets(nacks) }

// SetNACKRTTForTest sets the sequencer RTT, which sizes the NACK dedupe window min(100 ms, 2*rtt).
func (d *DownTrack) SetNACKRTTForTest(rtt uint32) { d.sequencer.setRTT(rtt) }

func (d *DownTrack) ForwarderCheckSyncForTest() (bool, int32) { return d.forwarder.CheckSync() }

func (d *DownTrack) IsNACKThrottledForTest() bool { return d.isNACKThrottled.Load() }

func (d *DownTrack) RTXCurbBoundaryForTest() uint64 { return d.rtxCurbBoundary.Load() }

func (d *DownTrack) RTXCurbedForTest() uint32 { return d.rtxCurbed.Load() }

func (d *DownTrack) RTXUncappedForTest() uint32 { return d.rtxUncapped.Load() }

const RTXCurbKeyFrameRepairWindowForTest = rtxCurbKeyFrameRepairWindow

// AgeRTXCurbKeyFrameMarkForTest backdates the key frame mark so it was set age ago.
func (d *DownTrack) AgeRTXCurbKeyFrameMarkForTest(age time.Duration) {
	d.rtxCurbKeyFrameAt.Store(mono.UnixNano() - int64(age))
}

// SetRTXCurbBoundaryForTest stands in for a commit that raced the key frame branch.
func (d *DownTrack) SetRTXCurbBoundaryForTest(b uint64) { d.rtxCurbBoundary.Store(b) }

// SetRTXCurbOnTemporalDownStepForTest sets the config gate; the shared harness builds DownTrackParams itself.
func (d *DownTrack) SetRTXCurbOnTemporalDownStepForTest(on bool) {
	d.params.RTXCurbOnTemporalDownStep = on
}

// SetKeyFrameOnTemporalDownStepForTest sets the config gate; the shared harness builds DownTrackParams itself.
func (d *DownTrack) SetKeyFrameOnTemporalDownStepForTest(on bool) {
	d.params.KeyFrameOnTemporalDownStep = on
}
