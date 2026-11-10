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

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/service"
)

const rpdLogPrefix = "debug remove participant delay"

type rpdEntry struct {
	msg string
	kv  map[string]any
}

type rpdLog struct {
	mu      sync.Mutex
	entries []rpdEntry
}

func (l *rpdLog) add(msg string, keysAndValues []any) {
	kv := make(map[string]any, len(keysAndValues)/2)
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if k, ok := keysAndValues[i].(string); ok {
			kv[k] = keysAndValues[i+1]
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, rpdEntry{msg: msg, kv: kv})
}

// find returns the hook entries with message msg whose field key equals value.
func (l *rpdLog) find(msg, key string, value any) []rpdEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []rpdEntry
	for _, e := range l.entries {
		if e.msg == msg && e.kv[key] == value {
			out = append(out, e)
		}
	}
	return out
}

func (l *rpdLog) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if e.msg == msg {
			n++
		}
	}
	return n
}

// rpdCaptureLogger records the hook's lines and discards everything else.
type rpdCaptureLogger struct {
	logger.Logger
	log *rpdLog
}

func (c rpdCaptureLogger) Infow(msg string, keysAndValues ...any) {
	if strings.HasPrefix(msg, rpdLogPrefix) {
		c.log.add(msg, keysAndValues)
	}
}
func (c rpdCaptureLogger) WithName(string) logger.Logger      { return c }
func (c rpdCaptureLogger) WithCallDepth(int) logger.Logger    { return c }
func (c rpdCaptureLogger) WithValues(...any) logger.Logger    { return c }
func (c rpdCaptureLogger) WithComponent(string) logger.Logger { return c }

var rpdCaptured = &rpdLog{}

// Set before TestMain and before any test goroutine reads the logger globals,
// so swapping the logger is not a data race.
func init() {
	logger.SetLogger(rpdCaptureLogger{Logger: logger.GetDiscardLogger(), log: rpdCaptured}, "rpd-test")
}

const (
	rpdEntryMsg = rpdLogPrefix
	rpdDoneMsg  = rpdLogPrefix + " done"
	rpdArmedMsg = rpdLogPrefix + " armed"
)

func newRPDRoomManager(t *testing.T, marker string) *service.RoomManager {
	conf, err := config.NewConfig(`room:
  debug_remove_participant_delay:
    room_marker: "`+marker+`"
    max_delay: 5s`, true, nil, nil)
	require.NoError(t, err)
	return newTestRoomManager(t, conf, &fakeEgressLauncher{})
}

func joinRPDParticipant(t *testing.T, rm *service.RoomManager, roomName string, identity livekit.ParticipantIdentity) (*rtc.Room, types.LocalParticipant) {
	ctx := context.Background()
	room := rm.GetRoom(ctx, livekit.RoomName(roomName))
	if room == nil {
		_, err := rm.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: roomName})
		require.NoError(t, err)
		room = rm.GetRoom(ctx, livekit.RoomName(roomName))
		require.NotNil(t, room)
	}
	p := rtc.NewMockParticipant(identity, types.CurrentProtocol, false, true, room.LocalParticipantListener())
	require.NoError(t, room.Join(p, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil))
	return room, p
}

type rpdResult struct {
	err     error
	elapsed time.Duration
}

func removeAsync(rm *service.RoomManager, roomName string, identity livekit.ParticipantIdentity) (time.Time, <-chan rpdResult) {
	start := time.Now()
	ch := make(chan rpdResult, 1)
	go func() {
		_, err := rm.RemoveParticipant(context.Background(), &livekit.RoomParticipantIdentity{Room: roomName, Identity: string(identity)})
		ch <- rpdResult{err: err, elapsed: time.Since(start)}
	}()
	return start, ch
}

// requireEntryDone checks the room has exactly one entry and one done line,
// sharing seq, and returns them.
func requireEntryDone(t *testing.T, roomName string) (rpdEntry, rpdEntry) {
	t.Helper()
	entries := rpdCaptured.find(rpdEntryMsg, "room", roomName)
	require.Len(t, entries, 1)
	seq := entries[0].kv["seq"]
	require.NotNil(t, seq)
	dones := rpdCaptured.find(rpdDoneMsg, "room", roomName)
	require.Len(t, dones, 1)
	require.Equal(t, seq, dones[0].kv["seq"])
	return entries[0], dones[0]
}

func requirePSRPCCode(t *testing.T, err error, code psrpc.ErrorCode) {
	t.Helper()
	var pe psrpc.Error
	require.True(t, errors.As(err, &pe), "not a psrpc error: %v", err)
	require.Equal(t, code, pe.Code())
}

func TestRemoveParticipantDelay_UnmarkedRoom(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	ctx := context.Background()

	start := time.Now()
	_, err := rm.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "e2e-unmarked-missing", Identity: "guest"})
	require.Less(t, time.Since(start), 5*time.Millisecond)
	require.ErrorIs(t, err, service.ErrRoomNotFound)

	room, _ := joinRPDParticipant(t, rm, "e2e-unmarked-present", "guest")
	_, err = rm.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "e2e-unmarked-present", Identity: "guest"})
	require.NoError(t, err)
	require.Nil(t, room.GetParticipant("guest"))

	for _, name := range []string{"e2e-unmarked-missing", "e2e-unmarked-present"} {
		require.Empty(t, rpdCaptured.find(rpdEntryMsg, "room", name))
		require.Empty(t, rpdCaptured.find(rpdDoneMsg, "room", name))
	}
}

func TestRemoveParticipantDelay_MarkedMissingRoom(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	const roomName = "e2e-missing-rpd100-1"

	start := time.Now()
	_, err := rm.RemoveParticipant(context.Background(), &livekit.RoomParticipantIdentity{Room: roomName, Identity: "guest"})
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	require.ErrorIs(t, err, service.ErrRoomNotFound)

	entry, done := requireEntryDone(t, roomName)
	require.Equal(t, "guest", entry.kv["participant"])
	require.Equal(t, 100*time.Millisecond, entry.kv["delay"])
	require.Equal(t, "delay", entry.kv["mode"])
	require.Equal(t, "", entry.kv["holder_sid"])
	require.Equal(t, "not_found", done.kv["outcome"])
}

// The faithful-path pin: the delay runs BEFORE the identity lookup, so a
// participant that leaves during it is not found.
func TestRemoveParticipantDelay_ParticipantLeavesDuringDelay(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	const roomName = "e2e-leaves-rpd300-1"
	room, p := joinRPDParticipant(t, rm, roomName, "guest")

	_, ch := removeAsync(rm, roomName, "guest")
	require.Eventually(t, func() bool { return len(rpdCaptured.find(rpdEntryMsg, "room", roomName)) == 1 }, time.Second, time.Millisecond)
	room.RemoveParticipant("guest", p.ID(), types.ParticipantCloseReasonClientRequestLeave)

	res := <-ch
	require.ErrorIs(t, res.err, service.ErrParticipantNotFound)
	require.GreaterOrEqual(t, res.elapsed, 300*time.Millisecond)

	entry, done := requireEntryDone(t, roomName)
	require.Equal(t, string(p.ID()), entry.kv["holder_sid"])
	require.Equal(t, 300*time.Millisecond, entry.kv["delay"])
	require.Equal(t, "not_found", done.kv["outcome"])
}

func TestRemoveParticipantDelay_HolderStays(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	const roomName = "e2e-stays-rpd50-1"
	room, p := joinRPDParticipant(t, rm, roomName, "guest")

	_, ch := removeAsync(rm, roomName, "guest")
	res := <-ch
	require.NoError(t, res.err)
	require.GreaterOrEqual(t, res.elapsed, 50*time.Millisecond)
	require.Nil(t, room.GetParticipant("guest"))

	_, done := requireEntryDone(t, roomName)
	require.Equal(t, "removing "+string(p.ID()), done.kv["outcome"])
}

func TestRemoveParticipantDelay_SeqPerRequest(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	const roomName = "e2e-seq-rpd10-1"
	for range 2 {
		_, err := rm.RemoveParticipant(context.Background(), &livekit.RoomParticipantIdentity{Room: roomName, Identity: "guest"})
		require.ErrorIs(t, err, service.ErrRoomNotFound)
	}
	entries := rpdCaptured.find(rpdEntryMsg, "room", roomName)
	dones := rpdCaptured.find(rpdDoneMsg, "room", roomName)
	require.Len(t, entries, 2)
	require.Len(t, dones, 2)
	require.NotEqual(t, entries[0].kv["seq"], entries[1].kv["seq"])
	require.Equal(t, entries[0].kv["seq"], dones[0].kv["seq"])
	require.Equal(t, entries[1].kv["seq"], dones[1].kv["seq"])
}

func TestRemoveParticipantDelay_DropHolderUnchanged(t *testing.T) {
	rm := newRPDRoomManager(t, "-rpd")
	const roomName = "e2e-dropstays-rpdd50-1"
	room, p := joinRPDParticipant(t, rm, roomName, "guest")

	_, ch := removeAsync(rm, roomName, "guest")
	res := <-ch
	require.NoError(t, res.err)
	require.GreaterOrEqual(t, res.elapsed, 50*time.Millisecond)
	require.Less(t, res.elapsed, 3*time.Second)
	require.Nil(t, room.GetParticipant("guest"))

	entry, done := requireEntryDone(t, roomName)
	require.Equal(t, "drop", entry.kv["mode"])
	require.Equal(t, "removing "+string(p.ID()), done.kv["outcome"])
}

// Takes 3.5 s per subtest by design: run it on its own with -run Drop_Holder.
func TestRemoveParticipantDelayDrop_HolderGoneOrReplaced(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "gone"
		if replace {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			rm := newRPDRoomManager(t, "-rpd")
			roomName := "e2e-drop" + name + "-rpdd100-1"
			room, p := joinRPDParticipant(t, rm, roomName, "guest")

			_, ch := removeAsync(rm, roomName, "guest")
			require.Eventually(t, func() bool { return len(rpdCaptured.find(rpdEntryMsg, "room", roomName)) == 1 }, time.Second, time.Millisecond)
			room.RemoveParticipant("guest", p.ID(), types.ParticipantCloseReasonClientRequestLeave)
			var p2 types.LocalParticipant
			if replace {
				_, p2 = joinRPDParticipant(t, rm, roomName, "guest")
			}

			res := <-ch
			requirePSRPCCode(t, res.err, psrpc.Unavailable)
			require.GreaterOrEqual(t, res.elapsed, 3500*time.Millisecond)

			entry, done := requireEntryDone(t, roomName)
			require.Equal(t, "drop", entry.kv["mode"])
			require.Equal(t, string(p.ID()), entry.kv["holder_sid"])
			require.Equal(t, "dropped", done.kv["outcome"])
			if replace {
				// room.RemoveParticipant was not called: the newcomer is still seated
				require.Equal(t, p2.ID(), room.GetParticipant("guest").ID())
			}
		})
	}
}

func TestRemoveParticipantDelay_ArmedOnce(t *testing.T) {
	newRPDRoomManager(t, "-rpdarmed")
	armed := rpdCaptured.find(rpdArmedMsg, "room_marker", "-rpdarmed")
	require.Len(t, armed, 1)
	require.Equal(t, 5*time.Second, armed[0].kv["max_delay"])

	before := rpdCaptured.count(rpdArmedMsg)
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	newTestRoomManager(t, conf, &fakeEgressLauncher{})
	require.Equal(t, before, rpdCaptured.count(rpdArmedMsg))
}
