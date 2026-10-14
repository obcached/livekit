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

// queuingDelayBaseline is a sliding-window minimum of finalized packet group
// propagated queuing delays, keyed by group send time.
type queuingDelayBaseline struct{}

func newQueuingDelayBaseline(_ time.Duration) *queuingDelayBaseline {
	return &queuingDelayBaseline{}
}

func (q *queuingDelayBaseline) Push(_ int64, _ int64) {}

func (q *queuingDelayBaseline) Min() int64 {
	return 0
}

func (q *queuingDelayBaseline) Reset() {}
