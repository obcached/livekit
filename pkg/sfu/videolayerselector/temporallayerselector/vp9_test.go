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
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/protocol/logger"
)

func vp9Packet(tid uint8, b bool, u bool, marker bool) *buffer.ExtPacket {
	return &buffer.ExtPacket{
		VideoLayer: buffer.VideoLayer{Spatial: 0, Temporal: int32(tid)},
		Packet:     &rtp.Packet{Header: rtp.Header{Marker: marker}},
		Payload:    codecs.VP9Packet{TID: tid, B: b, U: u, E: marker},
	}
}

func ddPacket(tid int, first bool, marker bool) *buffer.ExtPacket {
	return &buffer.ExtPacket{
		VideoLayer: buffer.VideoLayer{Spatial: 0, Temporal: int32(tid)},
		Packet:     &rtp.Packet{Header: rtp.Header{Marker: marker}},
		DependencyDescriptor: &buffer.ExtDependencyDescriptor{
			Descriptor: &dd.DependencyDescriptor{
				FirstPacketInFrame: first,
				LastPacketInFrame:  marker,
				FrameDependencies:  &dd.FrameDependencyTemplate{TemporalId: tid},
			},
		},
	}
}

func TestVP9TemporalLayerSelector(t *testing.T) {
	s := NewVP9(logger.GetLogger())

	t.Run("at target", func(t *testing.T) {
		this, next := s.Select(vp9Packet(2, true, true, false), 1, 1)
		require.Equal(t, int32(1), this)
		require.Equal(t, int32(1), next)
	})

	t.Run("VP9 up-switch only on a switching up point", func(t *testing.T) {
		// not a switching up point
		this, next := s.Select(vp9Packet(1, true, false, false), 0, 2)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)

		// switching up point, but not the start of the frame
		this, next = s.Select(vp9Packet(1, false, true, false), 0, 2)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)

		// switching up point skipping a layer: it may reference a dropped frame of the layer in between
		this, next = s.Select(vp9Packet(2, true, true, false), 0, 2)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)

		// switching up point, one layer at a time
		this, next = s.Select(vp9Packet(1, true, true, false), 0, 2)
		require.Equal(t, int32(1), this)
		require.Equal(t, int32(1), next)
		this, next = s.Select(vp9Packet(2, true, true, false), 1, 2)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)
	})

	t.Run("up-switch on key frame", func(t *testing.T) {
		extPkt := vp9Packet(0, true, false, false)
		extPkt.IsKeyFrame = true
		this, next := s.Select(extPkt, 0, 2)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)
	})

	t.Run("down-switch at end of frame", func(t *testing.T) {
		this, next := s.Select(vp9Packet(2, true, true, false), 2, 0)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)

		this, next = s.Select(vp9Packet(2, false, true, true), 2, 0)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(0), next)
	})

	t.Run("dependency descriptor up-switch on temporal layer 0 frame start", func(t *testing.T) {
		// higher layer frame start is not a switch point
		this, next := s.Select(ddPacket(2, true, false), 0, 2)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)

		// middle of a temporal layer 0 frame is not a switch point
		this, next = s.Select(ddPacket(0, false, false), 0, 2)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)

		this, next = s.Select(ddPacket(0, true, false), 0, 2)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)
	})

	t.Run("dependency descriptor down-switch at end of frame", func(t *testing.T) {
		this, next := s.Select(ddPacket(1, true, false), 2, 1)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)

		this, next = s.Select(ddPacket(1, false, true), 2, 1)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(1), next)
	})

	t.Run("no temporal layer information", func(t *testing.T) {
		// e.g. AV1 without dependency descriptor: nothing to filter on, follow the target
		extPkt := &buffer.ExtPacket{Packet: &rtp.Packet{}}
		this, next := s.Select(extPkt, 0, 2)
		require.Equal(t, int32(2), this)
		require.Equal(t, int32(2), next)

		this, next = s.Select(extPkt, 2, 0)
		require.Equal(t, int32(0), this)
		require.Equal(t, int32(0), next)
	})
}
