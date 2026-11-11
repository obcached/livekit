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
	"net/http"
	"regexp"

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

// expectedSIDPattern is the shape of a participant SID (guid.New("PA_")).
var expectedSIDPattern = regexp.MustCompile(`^PA_[A-Za-z0-9]{1,60}$`)

type expectedSIDKey struct{}

// ExpectedSIDMiddleware stores the raw ExpectedSIDHeader value in the request
// ctx for RoomService.RemoveParticipant, which validates it. It fronts every
// API route, /rtc included, so it never rejects a request and never wraps the
// ResponseWriter (websocket hijack).
type ExpectedSIDMiddleware struct{}

func (ExpectedSIDMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if v := r.Header.Get(ExpectedSIDHeader); v != "" {
		r = r.WithContext(context.WithValue(r.Context(), expectedSIDKey{}, v))
	}
	next(w, r)
}

// expectedSIDFromRequest returns the raw header value the middleware stored,
// or "" when the request carried none.
func expectedSIDFromRequest(ctx context.Context) string {
	v, _ := ctx.Value(expectedSIDKey{}).(string)
	return v
}

func validExpectedSID(sid string) bool {
	return expectedSIDPattern.MatchString(sid)
}

// withExpectedSIDMetadata adds the expected SID to the outgoing psrpc request
// metadata.
func withExpectedSIDMetadata(ctx context.Context, sid string) context.Context {
	return metadata.AppendMetadataToOutgoingContext(ctx, ExpectedSIDMetadataKey, sid)
}
