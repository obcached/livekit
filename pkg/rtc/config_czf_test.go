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

package rtc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
)

func newZeroForwardFaultWebRTCConfig(t *testing.T, zf *config.ZeroForwardFaultConfig) *WebRTCConfig {
	t.Helper()
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	conf.RTC.TCPPort = 0
	conf.RTC.DebugZeroForwardFault = zf
	webRTCConfig, err := NewWebRTCConfig(conf)
	require.NoError(t, err)
	return webRTCConfig
}

// t11: the key reaches the subscriber config, and survives the congestion control rebuild
func TestWebRTCConfigCarriesZeroForwardFault(t *testing.T) {
	zf := &config.ZeroForwardFaultConfig{Suffix: "-czf", AfterSeconds: 8}
	c := newZeroForwardFaultWebRTCConfig(t, zf)
	require.Same(t, zf, c.Subscriber.DebugZeroForwardFault)
	require.Nil(t, c.Publisher.DebugZeroForwardFault)

	c.UpdateSubscriberConfig(config.CongestionControlConfig{UseSendSideBWE: true})
	require.Same(t, zf, c.Subscriber.DebugZeroForwardFault, "UpdateSubscriberConfig keeps the key")
}

// t11: key absent -> no downtrack is faulted
func TestZeroForwardFaultDecisionKeyAbsent(t *testing.T) {
	c := newZeroForwardFaultWebRTCConfig(t, nil)
	require.Nil(t, c.Subscriber.DebugZeroForwardFault)
	c.UpdateSubscriberConfig(config.CongestionControlConfig{})
	for i := 0; i < 3; i++ {
		require.Nil(t, c.Subscriber.zeroForwardFault(true, "rtc-t11-absent-czf", "TR_absent"))
	}
}

// t11: first video downtrack per (identity, track) faulted, the next one clean
func TestZeroForwardFaultDecisionFirstOnly(t *testing.T) {
	c := newZeroForwardFaultWebRTCConfig(t, &config.ZeroForwardFaultConfig{Suffix: "-czf", AfterSeconds: 8})
	require.Nil(t, c.Subscriber.zeroForwardFault(false, "rtc-t11-czf", "TR_audio"), "audio")
	require.Nil(t, c.Subscriber.zeroForwardFault(true, "rtc-t11-other", "TR_video"), "suffix mismatch")
	require.NotNil(t, c.Subscriber.zeroForwardFault(true, "rtc-t11-czf", "TR_video"), "first")
	require.Nil(t, c.Subscriber.zeroForwardFault(true, "rtc-t11-czf", "TR_video"), "replacement")
}
