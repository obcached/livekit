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
	"time"
)

type queuingDelayBaselineEntry struct {
	at  int64
	pqd int64
}

// queuingDelayBaseline is a sliding-window minimum of finalized packet group
// propagated queuing delays, keyed by group send time (micro seconds).
//
// It is a monotonic deque: entries are kept in increasing `at` and increasing `pqd`
// order, so the front is the window minimum.
//   - an `at` older than the last pushed one is stored at the last pushed `at`,
//     so the newest time never moves backwards and such a push evicts nothing by itself.
//   - eviction is strict: an entry at exactly newest - window stays.
type queuingDelayBaseline struct {
	window  int64
	entries []queuingDelayBaselineEntry
	lastAt  int64
}

func newQueuingDelayBaseline(window time.Duration) *queuingDelayBaseline {
	return &queuingDelayBaseline{
		window: window.Microseconds(),
	}
}

func (q *queuingDelayBaseline) Push(at int64, pqd int64) {
	if len(q.entries) != 0 && at < q.lastAt {
		at = q.lastAt
	}
	q.lastAt = at

	for len(q.entries) != 0 && q.entries[len(q.entries)-1].pqd >= pqd {
		q.entries = q.entries[:len(q.entries)-1]
	}
	q.entries = append(q.entries, queuingDelayBaselineEntry{at: at, pqd: pqd})

	evictBefore := at - q.window
	for len(q.entries) != 0 && q.entries[0].at < evictBefore {
		q.entries = q.entries[1:]
	}
}

// Min returns the minimum over the window, 0 when empty.
func (q *queuingDelayBaseline) Min() int64 {
	if len(q.entries) == 0 {
		return 0
	}
	return q.entries[0].pqd
}

func (q *queuingDelayBaseline) Reset() {
	q.entries = nil
	q.lastAt = 0
}
