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

package sfu_test

// This file only uses APIs that predate the forward stall check, so it can be copied
// onto an older base to compare WriteRTP cost before and after.

import (
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/mediatransportutil/pkg/codec"
	"github.com/livekit/protocol/logger"

	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	"github.com/livekit/livekit-server/pkg/sfu/sfufakes"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
)

// fakeTrackLocalContext binds a DownTrack without a peer connection.
type fakeTrackLocalContext struct {
	codecs []webrtc.RTPCodecParameters
	ssrc   webrtc.SSRC
}

func (c *fakeTrackLocalContext) CodecParameters() []webrtc.RTPCodecParameters { return c.codecs }
func (c *fakeTrackLocalContext) HeaderExtensions() []webrtc.RTPHeaderExtensionParameter {
	return nil
}
func (c *fakeTrackLocalContext) SSRC() webrtc.SSRC                       { return c.ssrc }
func (c *fakeTrackLocalContext) SSRCRetransmission() webrtc.SSRC         { return 0 }
func (c *fakeTrackLocalContext) SSRCForwardErrorCorrection() webrtc.SSRC { return 0 }
func (c *fakeTrackLocalContext) WriteStream() webrtc.TrackLocalWriter    { return discardWriter{} }
func (c *fakeTrackLocalContext) ID() string                              { return "fake-track" }
func (c *fakeTrackLocalContext) RTCPReader() interceptor.RTCPReader      { return nil }

type discardWriter struct{}

func (discardWriter) WriteRTP(_ *rtp.Header, payload []byte) (int, error) { return len(payload), nil }
func (discardWriter) Write(b []byte) (int, error)                         { return len(b), nil }

// newUnwiredDownTrack builds a real DownTrack bound to a fake context, with packets
// discarded by the pacer. bind=false leaves it unbound.
func newUnwiredDownTrack(tb testing.TB, params DownTrackParams, bind bool) (*DownTrack, *sfufakes.FakeTrackReceiver) {
	tb.Helper()
	rcv := newFakeTrackReceiver(params.Codecs[0])
	params.Receiver = rcv
	if params.BufferFactory == nil {
		params.BufferFactory = buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	}
	if params.Pacer == nil {
		p := discardPacer{pacer.NewPassThrough(logger.GetLogger(), newNullBWE())}
		tb.Cleanup(p.Stop)
		params.Pacer = p
	}
	if params.Logger == nil {
		params.Logger = logger.GetLogger()
	}
	if params.Listener == nil {
		params.Listener = &sfufakes.FakeDownTrackListener{}
	}
	params.StreamID = "fake-stream"
	params.SubID = "fake-sub"
	params.MaxTrack = 500
	dt, err := NewDownTrack(params)
	require.NoError(tb, err)
	tb.Cleanup(dt.Close)
	if bind {
		_, err = dt.Bind(&fakeTrackLocalContext{codecs: params.Codecs, ssrc: 0x55667788})
		require.NoError(tb, err)
		dt.SetConnected()
		require.True(tb, dt.IsWritableForTest(), "downtrack should be writable")
	}
	return dt, rcv
}

// vp8KeyFramePacket returns the i-th VP8 key frame packet of a 30 fps stream.
func vp8KeyFramePacket(tb testing.TB, i int, arrival int64) *buffer.ExtPacket {
	tb.Helper()
	ep, err := testutils.GetTestExtPacketVP8(&testutils.TestExtPacketParams{
		SequenceNumber: uint16(5000 + i),
		Timestamp:      uint32(180000 + i*3000),
		SSRC:           0x33333333,
		PayloadType:    96,
		PayloadSize:    50,
		IsKeyFrame:     true,
		Marker:         true,
	}, &codec.VP8{FirstByte: 0x10, S: true, PictureID: uint16(1 + i), IsKeyFrame: true})
	require.NoError(tb, err)
	ep.Arrival = arrival
	return ep
}

func benchmarkDownTrackWriteRTP(b *testing.B, params DownTrackParams) {
	params.Codecs = []webrtc.RTPCodecParameters{vp8CodecParams}
	dt, _ := newUnwiredDownTrack(b, params, true)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})

	ep := vp8KeyFramePacket(b, 0, 1)
	require.Equal(b, int32(1), dt.WriteRTP(ep, 0), "first packet should forward")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ep.Packet.SequenceNumber++
		ep.ExtSequenceNumber++
		ep.Packet.Timestamp += 3000
		ep.ExtTimestamp += 3000
		ep.Arrival++
		dt.WriteRTP(ep, 0)
	}
}

func BenchmarkDownTrackWriteRTPHookOff(b *testing.B) {
	benchmarkDownTrackWriteRTP(b, DownTrackParams{})
}
