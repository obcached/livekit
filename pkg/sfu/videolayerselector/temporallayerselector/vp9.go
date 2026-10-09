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

package temporallayerselector

import (
	"github.com/pion/rtp/codecs"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/logger"
)

// VP9 selects the temporal layer of VP9 simulcast streams (one spatial layer per stream), using
// the dependency descriptor when present, else the VP9 payload descriptor.
type VP9 struct {
	logger logger.Logger
}

func NewVP9(logger logger.Logger) *VP9 {
	return &VP9{
		logger: logger,
	}
}

func (v *VP9) Select(extPkt *buffer.ExtPacket, current int32, target int32) (this int32, next int32) {
	this = current
	next = current
	if current == target {
		return
	}

	var isSwitchUpPoint bool
	tid := extPkt.Temporal
	if ddVal := extPkt.DependencyDescriptor; ddVal != nil && ddVal.Descriptor != nil {
		// frames following a temporal layer 0 frame do not reference anything before it,
		// so all layers up to the target can be forwarded from its start
		isSwitchUpPoint = ddVal.Descriptor.FirstPacketInFrame && tid == 0
		if isSwitchUpPoint {
			tid = target
		}
	} else if vp9, ok := extPkt.Payload.(codecs.VP9Packet); ok {
		// Switching up point, one layer at a time. libwebrtc (non-flexible mode, GOF 0, 2, 1, 2)
		// sets U on every frame: a temporal layer 2 frame following a dropped temporal layer 1
		// frame references it, only the next layer up is safe.
		isSwitchUpPoint = vp9.B && vp9.U && tid == current+1
	} else {
		// no temporal layer information, all temporal layers are forwarded
		this = target
		next = target
		return
	}

	if current < target {
		switch {
		case extPkt.IsKeyFrame:
			this = target
			next = target
		case isSwitchUpPoint && tid <= target:
			this = tid
			next = tid
		}
	} else {
		if extPkt.Packet.Marker {
			next = target
		}
	}
	return
}
