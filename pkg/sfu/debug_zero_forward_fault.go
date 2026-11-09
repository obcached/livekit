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

package sfu

import (
	"strings"
	"sync"
	"time"

	"github.com/livekit/protocol/utils/mono"
)

// ZeroForwardFault is a debug-only fault: once a bound DownTrack has been bound for
// `after`, WriteRTP drops every packet before the forwarder sees it. It reproduces a
// downtrack that is offered packets but forwards none. It is immutable.
type ZeroForwardFault struct {
	afterNanos int64
}

func NewZeroForwardFault(after time.Duration) *ZeroForwardFault {
	return &ZeroForwardFault{afterNanos: int64(after)}
}

// active compares two mono.UnixNano() values: the packet arrival and the bind time.
func (f *ZeroForwardFault) active(arrival int64, boundAt int64) bool {
	return boundAt != 0 && arrival-boundAt >= f.afterNanos
}

// SetZeroForwardFault sets the debug fault (nil = off). Call it after NewDownTrack and
// before Bind: WriteRTP reads it without a lock once Bind has made the track writable.
func (d *DownTrack) SetZeroForwardFault(f *ZeroForwardFault) {
	d.zeroForwardFault = f
}

// -------------------------------------------------------------------

const (
	zeroForwardFaultRegistryMax = 4096
	zeroForwardFaultRegistryTTL = 6 * time.Hour
)

// zeroForwardFaultRegistry remembers which (subscriber identity, track) pairs already
// got a faulted downtrack, so that a re-subscription gets a clean one.
type zeroForwardFaultRegistry struct {
	mu   sync.Mutex
	seen map[string]int64 // key -> mono nanos of the faulted downtrack
}

var zeroForwardFaults = &zeroForwardFaultRegistry{seen: make(map[string]int64)}

// first reports whether key is new, and records it. Entries expire after the TTL;
// when the registry is full of live entries, no further downtrack is faulted.
func (r *zeroForwardFaultRegistry) first(key string, now int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if at, ok := r.seen[key]; ok && now-at < int64(zeroForwardFaultRegistryTTL) {
		return false
	}
	if len(r.seen) >= zeroForwardFaultRegistryMax {
		for k, at := range r.seen {
			if now-at >= int64(zeroForwardFaultRegistryTTL) {
				delete(r.seen, k)
			}
		}
		if len(r.seen) >= zeroForwardFaultRegistryMax {
			return false
		}
	}
	r.seen[key] = now
	return true
}

// ZeroForwardFaultFor returns the fault for the first video downtrack of a subscriber
// whose identity ends with suffix, per track; nil for every other downtrack.
func ZeroForwardFaultFor(suffix string, after time.Duration, isVideo bool, identity string, trackID string) *ZeroForwardFault {
	if suffix == "" || after <= 0 || !isVideo || !strings.HasSuffix(identity, suffix) {
		return nil
	}
	if !zeroForwardFaults.first(identity+"|"+trackID, mono.UnixNano()) {
		return nil
	}
	return NewZeroForwardFault(after)
}
