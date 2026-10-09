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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/logger"
)

func TestTemporalFilter(t *testing.T) {
	tf := NewTemporalFilter(logger.GetLogger())

	// in order, out-of-order (late) and after a gap alike: the decision is on the temporal layer
	// only, there is no picture state to keep consistent (picture ids are not rewritten)
	for _, ordering := range []struct{ outOfOrder, hasGap bool }{{false, false}, {true, false}, {false, true}} {
		for tid := int32(0); tid <= 2; tid++ {
			extPkt := &buffer.ExtPacket{VideoLayer: buffer.VideoLayer{Temporal: tid}}

			inputSize, _, outputSize, err := tf.UpdateAndGet(extPkt, ordering.outOfOrder, ordering.hasGap, 1)
			if tid > 1 {
				require.ErrorIs(t, err, ErrFilteredTemporalLayer)
			} else {
				require.NoError(t, err)
			}
			// nothing to munge in the payload
			require.Zero(t, inputSize)
			require.Zero(t, outputSize)
		}
	}
}
