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

	"github.com/stretchr/testify/require"
)

func TestConfig_DebugZeroForwardFaultAbsentIsNil(t *testing.T) {
	conf, err := NewConfig("", true, nil, nil)
	require.NoError(t, err)
	require.Nil(t, conf.RTC.DebugZeroForwardFault)
}

func TestConfig_DebugZeroForwardFaultParsed(t *testing.T) {
	const content = `rtc:
  debug_zero_forward_fault:
    suffix: "-czf"
    after_seconds: 8`
	conf, err := NewConfig(content, true, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conf.RTC.DebugZeroForwardFault)
	require.Equal(t, "-czf", conf.RTC.DebugZeroForwardFault.Suffix)
	require.Equal(t, float64(8), conf.RTC.DebugZeroForwardFault.AfterSeconds)
}

func TestConfig_DebugZeroForwardFaultValidated(t *testing.T) {
	for name, content := range map[string]string{
		"empty suffix": `rtc:
  debug_zero_forward_fault:
    after_seconds: 8`,
		"zero after": `rtc:
  debug_zero_forward_fault:
    suffix: "-czf"`,
		"negative after": `rtc:
  debug_zero_forward_fault:
    suffix: "-czf"
    after_seconds: -1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewConfig(content, true, nil, nil)
			require.ErrorContains(t, err, "debug_zero_forward_fault")
		})
	}
}

func TestConfig_DebugZeroForwardFaultUnknownSubKeyRejected(t *testing.T) {
	const content = `rtc:
  debug_zero_forward_fault:
    suffix: "-czf"
    after_seconds: 8
    typo: 1`
	_, err := NewConfig(content, true, nil, nil)
	require.Error(t, err)
}
