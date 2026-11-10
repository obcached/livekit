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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig_DebugRemoveParticipantDelayAbsentIsNil(t *testing.T) {
	conf, err := NewConfig("", true, nil, nil)
	require.NoError(t, err)
	require.Nil(t, conf.Room.DebugRemoveParticipantDelay)
}

func TestConfig_DebugRemoveParticipantDelayParsed(t *testing.T) {
	const content = `room:
  debug_remove_participant_delay:
    room_marker: "-rpd"
    max_delay: 5s`
	conf, err := NewConfig(content, true, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conf.Room.DebugRemoveParticipantDelay)
	require.Equal(t, "-rpd", conf.Room.DebugRemoveParticipantDelay.RoomMarker)
	require.Equal(t, 5*time.Second, conf.Room.DebugRemoveParticipantDelay.MaxDelay)
}

func TestConfig_DebugRemoveParticipantDelayUnknownSubKeyRejected(t *testing.T) {
	const content = `room:
  debug_remove_participant_delay:
    room_marker: "-rpd"
    max_delay: 5s
    typo: 1`
	_, err := NewConfig(content, true, nil, nil)
	require.Error(t, err)
}

func TestConfig_DebugRemoveParticipantDelayValidated(t *testing.T) {
	for name, content := range map[string]string{
		"empty marker": `room:
  debug_remove_participant_delay:
    max_delay: 5s`,
		"zero max": `room:
  debug_remove_participant_delay:
    room_marker: "-rpd"`,
		"negative max": `room:
  debug_remove_participant_delay:
    room_marker: "-rpd"
    max_delay: -1s`,
		"max over 10s": `room:
  debug_remove_participant_delay:
    room_marker: "-rpd"
    max_delay: 10001ms`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewConfig(content, true, nil, nil)
			require.ErrorContains(t, err, "debug_remove_participant_delay")
		})
	}
}

func TestRemoveParticipantDelayConfig_Validate(t *testing.T) {
	var nilConf *RemoveParticipantDelayConfig
	require.NoError(t, nilConf.Validate())
	require.NoError(t, (&RemoveParticipantDelayConfig{RoomMarker: "-rpd", MaxDelay: 10 * time.Second}).Validate())
	require.NoError(t, (&RemoveParticipantDelayConfig{RoomMarker: "-rpd", MaxDelay: time.Millisecond}).Validate())
	require.Error(t, (&RemoveParticipantDelayConfig{MaxDelay: time.Second}).Validate())
	require.Error(t, (&RemoveParticipantDelayConfig{RoomMarker: "-rpd"}).Validate())
	require.Error(t, (&RemoveParticipantDelayConfig{RoomMarker: "-rpd", MaxDelay: -time.Second}).Validate())
	require.Error(t, (&RemoveParticipantDelayConfig{RoomMarker: "-rpd", MaxDelay: 10*time.Second + 1}).Validate())
}

func TestRemoveParticipantDelayConfig_Parse(t *testing.T) {
	conf := &RemoveParticipantDelayConfig{RoomMarker: "-rpd", MaxDelay: 5 * time.Second}

	for _, tc := range []struct {
		room  string
		delay time.Duration
		drop  bool
	}{
		{room: "e2e-takeover-cooldown-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpd30-20261111-ab12", delay: 30 * time.Millisecond},
		{room: "e2e-takeover-cooldown-rpd100-20261111-ab12", delay: 100 * time.Millisecond},
		{room: "e2e-takeover-cooldown-rpd30-20261111-ab12-guest", delay: 30 * time.Millisecond},
		{room: "e2e-takeover-cooldown-rpdd30-20261111-ab12", delay: 30 * time.Millisecond, drop: true},
		{room: "e2e-takeover-cooldown-rpd2500-20261111-ab12", delay: 2500 * time.Millisecond},
		{room: "e2e-takeover-cooldown-rpd9000-20261111-ab12", delay: 5 * time.Second},
		{room: "e2e-takeover-cooldown-rpdd9000-20261111-ab12", delay: 5 * time.Second, drop: true},
		{room: "e2e-takeover-cooldown-rpd99999999999999999999999-x", delay: 5 * time.Second},
		{room: "e2e-takeover-cooldown-rpdx-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpdd-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpd-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpd30x-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpddd30-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpd0-20261111-ab12"},
		{room: "e2e-takeover-cooldown-rpd30"},
		{room: "e2e-takeover-cooldown-rpdd30"},
		{room: "e2e-takeover-cooldown-RPD30-20261111-ab12"},
		{room: "e2e-rpdx-takeover-rpd40-20261111-ab12", delay: 40 * time.Millisecond},
	} {
		t.Run(tc.room, func(t *testing.T) {
			delay, drop := conf.Parse(tc.room)
			require.Equal(t, tc.delay, delay)
			require.Equal(t, tc.drop, drop)
		})
	}

	var nilConf *RemoveParticipantDelayConfig
	delay, drop := nilConf.Parse("e2e-takeover-cooldown-rpd100-20261111-ab12")
	require.Zero(t, delay)
	require.False(t, drop)
}
