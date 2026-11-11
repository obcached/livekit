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

package service

import (
	"context"

	"github.com/livekit/psrpc/pkg/metadata"
)

// An expected participant SID turns RemoveParticipant into a compare-and-remove:
// the handler removes the identity only while that SID holds it. The caller
// sends it as an HTTP header; the API node forwards it as psrpc request
// metadata to the node that hosts the participant. Without it, RemoveParticipant
// removes whoever holds the identity, as before.
const (
	ExpectedSIDHeader      = "X-Talkizy-Expected-Sid"
	ExpectedSIDMetadataKey = "x-talkizy-expected-sid"
)

// expectedSIDFromIncoming returns the expected SID carried by the psrpc
// request metadata, or "" when there is none.
func expectedSIDFromIncoming(ctx context.Context) string {
	head := metadata.IncomingHeader(ctx)
	if head == nil {
		return ""
	}
	return head.Metadata[ExpectedSIDMetadataKey]
}
