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

package service_test

// This file must sort after roommanager_rpd_test.go: its init wraps the
// capture logger that file's init installs, so both see their lines.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/mediatransportutil/pkg/rtcconfig"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/must"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/metadata"
	"github.com/livekit/psrpc/pkg/middleware/otelpsrpc"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
)

const (
	sgExpectedSIDKey   = "x-talkizy-expected-sid"
	sgReplacedMsg      = "participant session replaced"
	sgRefusedMsg       = "remove participant refused: session replaced"
	sgRemovingMsg      = "removing participant"
	sgRPDMarker        = "-rpd"
	sgPSRPCCallTimeout = 5 * time.Second
)

// sgCaptured records the refusal line (global logger) and, through
// sgParticipantLogger, each mock participant's "removing participant" line.
var sgCaptured = &rpdLog{}

type sgGlobalLogger struct {
	logger.Logger
}

func (c sgGlobalLogger) Infow(msg string, keysAndValues ...any) {
	if msg == sgRefusedMsg {
		sgCaptured.add(msg, keysAndValues)
	}
	c.Logger.Infow(msg, keysAndValues...)
}
func (c sgGlobalLogger) WithName(string) logger.Logger      { return c }
func (c sgGlobalLogger) WithCallDepth(int) logger.Logger    { return c }
func (c sgGlobalLogger) WithValues(...any) logger.Logger    { return c }
func (c sgGlobalLogger) WithComponent(string) logger.Logger { return c }

// Runs after roommanager_rpd_test.go's init (file order), before any test
// goroutine reads the logger globals.
func init() {
	logger.SetLogger(sgGlobalLogger{Logger: logger.GetLogger()}, "sidguard-test")
}

// sgParticipantLogger stands in for a participant's logger, which in
// production carries room and participant ID fields.
type sgParticipantLogger struct {
	logger.Logger
	room string
	sid  string
}

func (l sgParticipantLogger) Infow(msg string, keysAndValues ...any) {
	sgCaptured.add(msg, append([]any{"room", l.room, "sid", l.sid}, keysAndValues...))
}
func (l sgParticipantLogger) WithName(string) logger.Logger      { return l }
func (l sgParticipantLogger) WithCallDepth(int) logger.Logger    { return l }
func (l sgParticipantLogger) WithValues(...any) logger.Logger    { return l }
func (l sgParticipantLogger) WithComponent(string) logger.Logger { return l }

// newSGRoomManager builds a RoomManager on bus, with the rpd hook armed for
// rooms marked sgRPDMarker.
func newSGRoomManager(t *testing.T, bus psrpc.MessageBus) (*service.RoomManager, *config.Config) {
	conf, err := config.NewConfig(`room:
  debug_remove_participant_delay:
    room_marker: "`+sgRPDMarker+`"
    max_delay: 5s`, true, nil, nil)
	require.NoError(t, err)
	conf.RTC.TCPPort = 0
	conf.RTC.UDPPort = rtcconfig.PortRange{}

	node, err := routing.NewLocalNode(conf)
	require.NoError(t, err)
	router := &routingfakes.FakeRouter{}
	router.GetNodeForRoomReturns(node.Clone(), nil)

	store := service.NewLocalStore()
	ra, err := service.NewRoomAllocator(conf, router, store)
	require.NoError(t, err)

	rm, err := service.NewLocalRoomManager(
		conf,
		store,
		node,
		router,
		ra,
		&telemetryfakes.FakeTelemetryService{},
		nil,
		store,
		&fakeEgressLauncher{},
		utils.NewDefaultTimedVersionGenerator(),
		nil,
		bus,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(rm.Stop)
	return rm, conf
}

func joinSG(t *testing.T, rm *service.RoomManager, roomName string, identity livekit.ParticipantIdentity) (*rtc.Room, *typesfakes.FakeLocalParticipant) {
	t.Helper()
	ctx := context.Background()
	room := rm.GetRoom(ctx, livekit.RoomName(roomName))
	if room == nil {
		_, err := rm.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: roomName})
		require.NoError(t, err)
		room = rm.GetRoom(ctx, livekit.RoomName(roomName))
		require.NotNil(t, room)
	}
	p := rtc.NewMockParticipant(identity, types.CurrentProtocol, false, true, room.LocalParticipantListener())
	p.GetLoggerReturns(sgParticipantLogger{Logger: logger.GetDiscardLogger(), room: roomName, sid: string(p.ID())})
	require.NoError(t, room.Join(p, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil))
	return room, p
}

// withExpectedSID builds the handler ctx a psrpc server builds for a request
// whose metadata carries the expected SID.
func withExpectedSID(ctx context.Context, sid livekit.ParticipantID) context.Context {
	return metadata.NewContextWithIncomingHeader(ctx, &metadata.Header{
		Metadata: metadata.Metadata{sgExpectedSIDKey: string(sid)},
	})
}

func removeAsyncCtx(ctx context.Context, rm *service.RoomManager, roomName string, identity livekit.ParticipantIdentity) <-chan rpdResult {
	start := time.Now()
	ch := make(chan rpdResult, 1)
	go func() {
		_, err := rm.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: roomName, Identity: string(identity)})
		ch <- rpdResult{err: err, elapsed: time.Since(start)}
	}()
	return ch
}

func requireReplaced(t *testing.T, err error) {
	t.Helper()
	var pe psrpc.Error
	require.True(t, errors.As(err, &pe), "not a psrpc error: %v", err)
	require.Equal(t, psrpc.NotFound, pe.Code())
	require.Equal(t, sgReplacedMsg, pe.Error())
}

func sgRemovingLines(roomName string) int {
	return len(sgCaptured.find(sgRemovingMsg, "room", roomName))
}

// sgParticipantClient is a psrpc participant client built like
// getPSRPCClientParams (wire.go), otel client options included, so the
// metadata merge of the otel interceptor is exercised.
func sgParticipantClient(t *testing.T, conf *config.Config, bus psrpc.MessageBus) rpc.TypedParticipantClient {
	params := rpc.NewClientParams(conf.PSRPC, bus, logger.GetLogger(), rpc.PSRPCMetricsObserver{},
		otelpsrpc.ClientOptions(otelpsrpc.Config{}),
	)
	client, err := rpc.NewTypedParticipantClient(params)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

// serveParticipant registers a participant server for identity in roomName,
// as RoomManager.StartSession does (roommanager.go:582-585).
func serveParticipant(t *testing.T, rm *service.RoomManager, bus psrpc.MessageBus, roomName string, identity livekit.ParticipantIdentity) {
	topic := rpc.FormatParticipantTopic(livekit.RoomName(roomName), identity)
	srv := must.Get(rpc.NewTypedParticipantServer(rm, bus))
	require.NoError(t, srv.RegisterAllParticipantTopics(topic))
	t.Cleanup(srv.Kill)
}

// T-LK-0: the expected SID travels as psrpc request metadata from the client
// to the handler.
func TestRemoveParticipantSIDGuard_PSRPCMetadata(t *testing.T) {
	bus := psrpc.NewLocalMessageBus()
	rm, conf := newSGRoomManager(t, bus)
	client := sgParticipantClient(t, conf, bus)
	const identity = livekit.ParticipantIdentity("guest")

	call := func(roomName string, sid livekit.ParticipantID) error {
		ctx, cancel := context.WithTimeout(context.Background(), sgPSRPCCallTimeout)
		defer cancel()
		ctx = metadata.AppendMetadataToOutgoingContext(ctx, sgExpectedSIDKey, string(sid))
		_, err := client.RemoveParticipant(ctx, rpc.FormatParticipantTopic(livekit.RoomName(roomName), identity),
			&livekit.RoomParticipantIdentity{Room: roomName, Identity: string(identity)})
		return err
	}

	// pre-assertion: the topic is live, the right SID removes
	const liveRoom = "sg-psrpc-right"
	room, a := joinSG(t, rm, liveRoom, identity)
	serveParticipant(t, rm, bus, liveRoom, identity)
	require.NoError(t, call(liveRoom, a.ID()))
	require.Nil(t, room.GetParticipant(identity))
	require.Equal(t, 1, sgRemovingLines(liveRoom))

	// the wrong SID is refused over the bus, the participant stays
	const guardRoom = "sg-psrpc-wrong"
	room, a = joinSG(t, rm, guardRoom, identity)
	serveParticipant(t, rm, bus, guardRoom, identity)
	err := call(guardRoom, "PA_notTheLiveSession")
	requireReplaced(t, err)
	require.NotNil(t, room.GetParticipant(identity))
	require.Equal(t, a.ID(), room.GetParticipant(identity).ID())
	require.Equal(t, 0, sgRemovingLines(guardRoom))
}
