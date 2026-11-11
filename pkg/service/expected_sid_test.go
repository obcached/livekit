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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/metadata"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/service"
)

const removeParticipantPath = "/twirp/livekit.RoomService/RemoveParticipant"

// esRemoveThroughHandler sends one request through the node's HTTP handler
// (NewHTTPHandler, with or without the auth middleware) to an API handler that
// calls RoomService.RemoveParticipant with the request ctx plus admin grants.
// It returns the RemoveParticipant error.
func esRemoveThroughHandler(t *testing.T, svc *TestRoomService, keyProvider auth.KeyProvider, header []string) error {
	t.Helper()
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)

	var removeErr error
	reached := false
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		ctx := service.WithGrants(r.Context(), &auth.ClaimGrants{
			Video: &auth.VideoGrant{RoomAdmin: true, Room: "testroom"},
		}, "")
		_, removeErr = svc.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: "testroom", Identity: "guest"})
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, removeParticipantPath, strings.NewReader("{}"))
	for _, v := range header {
		req.Header.Add(service.ExpectedSIDHeader, v)
	}
	rec := httptest.NewRecorder()
	service.NewHTTPHandler(conf, keyProvider, api, nil).ServeHTTP(rec, req)
	require.True(t, reached)
	require.Equal(t, http.StatusOK, rec.Code)
	return removeErr
}

// esPublishedSID returns the expected SID of the published remove, and whether
// the outgoing metadata carries the key at all.
func esPublishedSID(t *testing.T, svc *TestRoomService) (string, bool) {
	t.Helper()
	require.Equal(t, 1, svc.participantClient.RemoveParticipantCallCount())
	ctx, _, _, _ := svc.participantClient.RemoveParticipantArgsForCall(0)
	sid, ok := metadata.OutgoingContextMetadata(ctx)[service.ExpectedSIDMetadataKey]
	return sid, ok
}

func esKeyProviders() map[string]auth.KeyProvider {
	return map[string]auth.KeyProvider{
		"no auth middleware":   nil,
		"with auth middleware": auth.NewSimpleKeyProvider("key", "secret"),
	}
}

// T-LK-8: header -> middleware -> ctx -> psrpc request metadata, whether or not
// the auth middleware is installed.
func TestExpectedSID_HeaderForwardedAsMetadata(t *testing.T) {
	for name, kp := range esKeyProviders() {
		t.Run(name, func(t *testing.T) {
			svc := newTestRoomService(config.LimitConfig{})
			require.NoError(t, esRemoveThroughHandler(t, svc, kp, []string{"PA_abc123XYZ"}))
			sid, ok := esPublishedSID(t, svc)
			require.True(t, ok)
			require.Equal(t, "PA_abc123XYZ", sid)
		})
	}
}

func TestExpectedSID_AbsentOrEmptyHeaderSendsNoKey(t *testing.T) {
	for name, header := range map[string][]string{"absent": nil, "empty": {""}} {
		t.Run(name, func(t *testing.T) {
			svc := newTestRoomService(config.LimitConfig{})
			require.NoError(t, esRemoveThroughHandler(t, svc, nil, header))
			_, ok := esPublishedSID(t, svc)
			require.False(t, ok)
		})
	}
}

func TestExpectedSID_MalformedHeaderIsInvalidArgument(t *testing.T) {
	for _, v := range []string{
		"bogus",
		"PA_",
		"pa_abc",
		"PA_abc-def",
		"PA_abc def",
		" PA_abc",
		"PA_" + strings.Repeat("a", 61),
	} {
		t.Run(v, func(t *testing.T) {
			svc := newTestRoomService(config.LimitConfig{})
			err := esRemoveThroughHandler(t, svc, nil, []string{v})
			requirePSRPCCode(t, err, psrpc.InvalidArgument)
			require.ErrorIs(t, err, service.ErrInvalidExpectedSID)
			require.Equal(t, 0, svc.participantClient.RemoveParticipantCallCount())
		})
	}
}

func TestExpectedSID_LongestValidSIDAccepted(t *testing.T) {
	svc := newTestRoomService(config.LimitConfig{})
	sidValue := "PA_" + strings.Repeat("a", 60)
	require.NoError(t, esRemoveThroughHandler(t, svc, nil, []string{sidValue}))
	sid, ok := esPublishedSID(t, svc)
	require.True(t, ok)
	require.Equal(t, sidValue, sid)
}

// The middleware fronts every API route, /rtc included: it never rejects a
// request and never wraps the ResponseWriter (websocket hijack).
func TestExpectedSID_MiddlewareNeverRejectsNorWraps(t *testing.T) {
	for _, path := range []string{"/rtc", "/rtc/v1", removeParticipantPath, "/"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set(service.ExpectedSIDHeader, "not a sid")
			called := false
			service.ExpectedSIDMiddleware{}.ServeHTTP(rec, req, func(w http.ResponseWriter, r *http.Request) {
				called = true
				require.Same(t, rec, w)
				w.WriteHeader(http.StatusTeapot)
			})
			require.True(t, called)
			require.Equal(t, http.StatusTeapot, rec.Code)
		})
	}

	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	reached := false
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/rtc", nil)
	req.Header.Set(service.ExpectedSIDHeader, "not a sid")
	rec := httptest.NewRecorder()
	service.NewHTTPHandler(conf, nil, api, nil).ServeHTTP(rec, req)
	require.True(t, reached)
	require.Equal(t, http.StatusOK, rec.Code)
}

// Only RemoveParticipant forwards the expected SID: a ctx that carries it does
// not change another RoomService call.
func TestExpectedSID_OtherRoutesIgnoreIt(t *testing.T) {
	svc := newTestRoomService(config.LimitConfig{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/twirp/livekit.RoomService/MutePublishedTrack", nil)
	req.Header.Set(service.ExpectedSIDHeader, "bogus")
	service.ExpectedSIDMiddleware{}.ServeHTTP(rec, req, func(_ http.ResponseWriter, r *http.Request) {
		ctx := service.WithGrants(r.Context(), &auth.ClaimGrants{
			Video: &auth.VideoGrant{RoomAdmin: true, Room: "testroom"},
		}, "")
		_, err := svc.MutePublishedTrack(ctx, &livekit.MuteRoomTrackRequest{Room: "testroom", Identity: "guest", TrackSid: "TR_x"})
		require.NoError(t, err)
	})
	require.Equal(t, 1, svc.participantClient.MutePublishedTrackCallCount())
	ctx, _, _, _ := svc.participantClient.MutePublishedTrackArgsForCall(0)
	_, ok := metadata.OutgoingContextMetadata(ctx)[service.ExpectedSIDMetadataKey]
	require.False(t, ok)
}

// The full API path: an authenticated Twirp RemoveParticipant through the
// node's HTTP handler publishes the expected SID; a malformed one is a Twirp
// invalid_argument and publishes nothing.
func TestExpectedSID_TwirpEndToEnd(t *testing.T) {
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	at := auth.NewAccessToken("key", "secret")
	at.SetVideoGrant(&auth.VideoGrant{RoomAdmin: true, Room: "testroom"})
	token, err := at.ToJWT()
	require.NoError(t, err)

	post := func(svc *TestRoomService, sid string) *httptest.ResponseRecorder {
		handler := service.NewHTTPHandler(conf, auth.NewSimpleKeyProvider("key", "secret"), livekit.NewRoomServiceServer(&svc.RoomService), nil)
		req := httptest.NewRequest(http.MethodPost, removeParticipantPath, strings.NewReader(`{"room":"testroom","identity":"guest"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set(service.ExpectedSIDHeader, sid)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	svc := newTestRoomService(config.LimitConfig{})
	svc.participantClient.RemoveParticipantReturns(&livekit.RemoveParticipantResponse{}, nil)
	rec := post(svc, "PA_abc123")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	sid, ok := esPublishedSID(t, svc)
	require.True(t, ok)
	require.Equal(t, "PA_abc123", sid)

	svc = newTestRoomService(config.LimitConfig{})
	rec = post(svc, "bogus")
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "invalid_argument")
	require.Equal(t, 0, svc.participantClient.RemoveParticipantCallCount())
}
