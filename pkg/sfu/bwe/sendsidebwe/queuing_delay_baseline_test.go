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

package sendsidebwe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// U9
func TestQueuingDelayBaseline(t *testing.T) {
	q := newQueuingDelayBaseline(10 * time.Millisecond) // 10_000 us
	require.Equal(t, int64(0), q.Min(), "empty")

	q.Push(1_000, 50)
	require.Equal(t, int64(50), q.Min())

	q.Push(2_000, 30)
	require.Equal(t, int64(30), q.Min())

	q.Push(3_000, 40)
	require.Equal(t, int64(30), q.Min())

	// out-of-order: stored at the last pushed time (3_000), evicts nothing
	q.Push(2_500, 35)
	require.Equal(t, int64(30), q.Min())

	// newest - window = 2_000: the entry at exactly 2_000 stays
	q.Push(12_000, 60)
	require.Equal(t, int64(30), q.Min())

	// newest - window = 2_001: the entry at 2_000 is evicted, the out-of-order one (stored at 3_000) is the min
	q.Push(12_001, 70)
	require.Equal(t, int64(35), q.Min())

	// newest - window = 2_600: an out-of-order entry kept at its own time (2_500) would be evicted here
	q.Push(12_600, 90)
	require.Equal(t, int64(35), q.Min())

	// newest - window = 3_001: entries at 3_000 are evicted
	q.Push(13_001, 80)
	require.Equal(t, int64(60), q.Min())

	// out-of-order far in the past: stored at 13_001, does not move newest back, evicts nothing
	q.Push(1, 100)
	require.Equal(t, int64(60), q.Min())
	q.Push(13_001, 65)
	require.Equal(t, int64(60), q.Min())

	q.Reset()
	require.Equal(t, int64(0), q.Min(), "empty after reset")
	q.Push(1, 5)
	require.Equal(t, int64(5), q.Min())
}
