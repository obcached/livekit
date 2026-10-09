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

package codecmunger

import (
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/logger"
)

// TemporalFilter drops packets above the selected temporal layer, for codecs which do not
// need any payload munging to do so (VP9 simulcast). It decides on the temporal layer only,
// out-of-order packets included.
//
// Like the VP9 SVC selector, VP9 picture ids and TL0PICIDX are left as is: in non-flexible mode
// the receiver (libwebrtc RtpVp9RefFinder) locates a frame in the GOF from its picture id distance
// to the last temporal layer 0 picture, rewriting picture ids would break that. Temporal layer 0
// is never dropped, so TL0PICIDX stays contiguous.
type TemporalFilter struct {
	*Null
}

func NewTemporalFilter(logger logger.Logger) *TemporalFilter {
	return &TemporalFilter{
		Null: NewNull(logger),
	}
}

func (t *TemporalFilter) UpdateAndGet(extPkt *buffer.ExtPacket, _snOutOfOrder bool, _snHasGap bool, maxTemporal int32) (int, [MaxHeaderSize]byte, int, error) {
	if extPkt.Temporal > maxTemporal {
		return 0, [MaxHeaderSize]byte{}, 0, ErrFilteredTemporalLayer
	}
	return 0, [MaxHeaderSize]byte{}, 0, nil
}
