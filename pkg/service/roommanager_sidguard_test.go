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
	p := newSGParticipant(room, roomName, identity)
	require.NoError(t, room.Join(p, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil))
	return room, p
}

// newSGParticipant returns a mock participant, its SID minted, not yet joined.
func newSGParticipant(room *rtc.Room, roomName string, identity livekit.ParticipantIdentity) *typesfakes.FakeLocalParticipant {
	p := rtc.NewMockParticipant(identity, types.CurrentProtocol, false, true, room.LocalParticipantListener())
	p.GetLoggerReturns(sgParticipantLogger{Logger: logger.GetDiscardLogger(), room: roomName, sid: string(p.ID())})
	return p
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

// waitEntry waits for the rpd hook entry line of roomName: the handler is
// inside its delay, before the identity lookup.
func waitEntry(t *testing.T, roomName string) {
	t.Helper()
	require.Eventually(t, func() bool { return len(rpdCaptured.find(rpdEntryMsg, "room", roomName)) == 1 }, time.Second, time.Millisecond)
}

func doneOutcome(t *testing.T, roomName string) any {
	t.Helper()
	_, done := requireEntryDone(t, roomName)
	return done.kv["outcome"]
}

// replaceDuringDelay seats A in a held room and starts a remove whose expected
// SID expect picks ("" = none). Inside the delay it closes A with reason and
// seats B under the same identity.
func replaceDuringDelay(
	t *testing.T,
	rm *service.RoomManager,
	roomName string,
	reason types.ParticipantCloseReason,
	expect func(a, b livekit.ParticipantID) livekit.ParticipantID,
) (*rtc.Room, *typesfakes.FakeLocalParticipant, *typesfakes.FakeLocalParticipant, rpdResult) {
	t.Helper()
	room, a := joinSG(t, rm, roomName, "guest")
	b := newSGParticipant(room, roomName, "guest")
	require.NotEqual(t, a.ID(), b.ID())
	ctx := context.Background()
	if sid := expect(a.ID(), b.ID()); sid != "" {
		ctx = withExpectedSID(ctx, sid)
	}
	ch := removeAsyncCtx(ctx, rm, roomName, "guest")
	waitEntry(t, roomName)
	room.RemoveParticipant("guest", a.ID(), reason)
	require.NoError(t, room.Join(b, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil))
	return room, a, b, <-ch
}

func expectA(a, _ livekit.ParticipantID) livekit.ParticipantID { return a }
func expectB(_, b livekit.ParticipantID) livekit.ParticipantID { return b }
func expectNone(_, _ livekit.ParticipantID) livekit.ParticipantID {
	return ""
}

func requireSeated(t *testing.T, room *rtc.Room, p types.LocalParticipant) {
	t.Helper()
	live := room.GetParticipant(p.Identity())
	require.NotNil(t, live)
	require.Equal(t, p.ID(), live.ID())
}

// T-LK-1 (O1): expected == live -> removed.
func TestRemoveParticipantSIDGuard_Match(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-match-rpd10-1"
	room, a := joinSG(t, rm, roomName, "guest")

	res := <-removeAsyncCtx(withExpectedSID(context.Background(), a.ID()), rm, roomName, "guest")
	require.NoError(t, res.err)
	require.Nil(t, room.GetParticipant("guest"))
	require.Equal(t, 1, sgRemovingLines(roomName))
	require.Equal(t, "removing "+string(a.ID()), doneOutcome(t, roomName))
	require.Empty(t, sgCaptured.find(sgRefusedMsg, "room", roomName))
}

// T-LK-2 (O3, WORLD K): the late handler finds a new session of the identity
// and refuses to remove it.
func TestRemoveParticipantSIDGuard_WorldK(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-worldk-rpd300-1"
	room, a, b, res := replaceDuringDelay(t, rm, roomName, types.ParticipantCloseReasonClientRequestLeave, expectA)

	requireReplaced(t, res.err)
	require.ErrorIs(t, res.err, service.ErrParticipantSessionReplaced)
	requireSeated(t, room, b)
	require.Equal(t, "sid_mismatch "+string(b.ID()), doneOutcome(t, roomName))
	require.Equal(t, 0, sgRemovingLines(roomName))

	refused := sgCaptured.find(sgRefusedMsg, "room", roomName)
	require.Len(t, refused, 1)
	require.Equal(t, "guest", refused[0].kv["participant"])
	require.Equal(t, string(a.ID()), refused[0].kv["expected_sid"])
	require.Equal(t, string(b.ID()), refused[0].kv["live_sid"])
}

// T-LK-3 (O5): no expected SID -> identity-keyed, the new session is removed
// (kick semantics).
func TestRemoveParticipantSIDGuard_NoExpectedRemovesReplacement(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-noexp-rpd300-1"
	room, _, b, res := replaceDuringDelay(t, rm, roomName, types.ParticipantCloseReasonClientRequestLeave, expectNone)

	require.NoError(t, res.err)
	require.Nil(t, room.GetParticipant("guest"))
	require.Equal(t, "removing "+string(b.ID()), doneOutcome(t, roomName))
	require.Equal(t, 1, sgRemovingLines(roomName))
	require.Empty(t, sgCaptured.find(sgRefusedMsg, "room", roomName))
}

// T-LK-4 (O2): A gone and nobody seated -> plain not found.
func TestRemoveParticipantSIDGuard_GoneNotFound(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-gone-rpd300-1"
	room, a := joinSG(t, rm, roomName, "guest")

	ch := removeAsyncCtx(withExpectedSID(context.Background(), a.ID()), rm, roomName, "guest")
	waitEntry(t, roomName)
	room.RemoveParticipant("guest", a.ID(), types.ParticipantCloseReasonClientRequestLeave)

	res := <-ch
	require.ErrorIs(t, res.err, service.ErrParticipantNotFound)
	require.NotErrorIs(t, res.err, service.ErrParticipantSessionReplaced)
	require.Equal(t, "participant does not exist", res.err.Error())
	require.Equal(t, "not_found", doneOutcome(t, roomName))
	require.Empty(t, sgCaptured.find(sgRefusedMsg, "room", roomName))
}

// T-LK-5 (O6): the caller targets the new session -> it is removed.
func TestRemoveParticipantSIDGuard_ExpectedIsReplacement(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-expb-rpd300-1"
	room, _, b, res := replaceDuringDelay(t, rm, roomName, types.ParticipantCloseReasonClientRequestLeave, expectB)

	require.NoError(t, res.err)
	require.Nil(t, room.GetParticipant("guest"))
	require.Equal(t, "removing "+string(b.ID()), doneOutcome(t, roomName))
}

// T-LK-6 (O4): A closed with the DUPLICATE_IDENTITY close reason, B seated
// during the delay -> refused. The harness uses room.Join, not StartSession,
// so roommanager.go's duplicate-identity branch itself is not exercised.
func TestRemoveParticipantSIDGuard_DuplicateIdentityClose(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-dupid-rpd300-1"
	room, _, b, res := replaceDuringDelay(t, rm, roomName, types.ParticipantCloseReasonDuplicateIdentity, expectA)

	requireReplaced(t, res.err)
	requireSeated(t, room, b)
	require.Equal(t, "sid_mismatch "+string(b.ID()), doneOutcome(t, roomName))
}

// T-LK-7 (O7): the identity changes hands between the lookup and the delete;
// the room-level guard under the room lock keeps B seated.
func TestRemoveParticipantSIDGuard_SwapAfterLookup(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-swap"
	room, a := joinSG(t, rm, roomName, "guest")
	b := newSGParticipant(room, roomName, "guest")

	// the hook runs on the handler goroutine; the channel receive below
	// orders its writes before the reads
	swapped := false
	var joinErr error
	service.SetRemoveParticipantAfterLookupHook(t, func() {
		swapped = true
		room.RemoveParticipant("guest", a.ID(), types.ParticipantCloseReasonClientRequestLeave)
		joinErr = room.Join(b, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil)
	})

	res := <-removeAsyncCtx(withExpectedSID(context.Background(), a.ID()), rm, roomName, "guest")
	require.True(t, swapped)
	require.NoError(t, joinErr)
	require.NoError(t, res.err)
	requireSeated(t, room, b)
}

// T-LK-9: drop mode with an expected SID keeps the drop path: the replaced
// holder is dropped before the guard runs.
func TestRemoveParticipantSIDGuard_DropModeUnchanged(t *testing.T) {
	rm, _ := newSGRoomManager(t, psrpc.NewLocalMessageBus())
	const roomName = "sg-drop-rpdd300-1"
	room, _, b, res := replaceDuringDelay(t, rm, roomName, types.ParticipantCloseReasonClientRequestLeave, expectA)

	requirePSRPCCode(t, res.err, psrpc.Unavailable)
	require.GreaterOrEqual(t, res.elapsed, 3500*time.Millisecond)
	requireSeated(t, room, b)
	require.Equal(t, "dropped", doneOutcome(t, roomName))
	require.Empty(t, sgCaptured.find(sgRefusedMsg, "room", roomName))
}

// T-LK-10: the wire contract shared with pnm.
func TestRemoveParticipantSIDGuard_GoldenConstants(t *testing.T) {
	require.Equal(t, "X-Talkizy-Expected-Sid", service.ExpectedSIDHeader)
	require.Equal(t, "x-talkizy-expected-sid", service.ExpectedSIDMetadataKey)
	require.Equal(t, sgExpectedSIDKey, service.ExpectedSIDMetadataKey)
	require.Equal(t, "participant session replaced", service.ErrParticipantSessionReplaced.Error())
	require.Equal(t, psrpc.NotFound, service.ErrParticipantSessionReplaced.Code())
}
