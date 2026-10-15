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

package sfu_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// stepPLIs commits a deficient temporal-only down-step s0t2 -> s0t1 and returns the SendPLI
// calls the commit made, each asserted to be (spatial 0, force=false).
func (r *stepRig) stepPLIs() int {
	before := r.dh.receiver.SendPLICallCount()
	r.stepDown(s0t1)
	after := r.dh.receiver.SendPLICallCount()
	for i := before; i < after; i++ {
		spatial, force := r.dh.receiver.SendPLIArgsForCall(i)
		require.EqualValues(r.t, 0, spatial)
		require.False(r.t, force, "PLI on temporal down-step must keep the receiver throttle")
	}
	return after - before
}

func TestDownTrackKeyFrameOnTemporalDownStep(t *testing.T) {
	t.Run("flag on: one PLI, NACK throttle untouched", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{keyFrame: true, layer: s0t2})
		r.preStep()
		require.False(t, r.dt.IsNACKThrottledForTest(), "precondition: not throttled")

		require.Equal(t, 1, r.stepPLIs())
		require.False(t, r.dt.IsNACKThrottledForTest(), "the down-step PLI must not stop RTX")
	})

	t.Run("both keys on: RTX curb still applies, post-step RTX continues", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{rtxCurb: true, keyFrame: true, layer: s0t2})
		sns := r.preStep()

		require.Equal(t, 1, r.stepPLIs())

		rtx, curbed := r.nack(sns[1])
		require.Zero(t, rtx, "pre-step packet must not be retransmitted")
		require.Equal(t, 1, curbed)

		r.writeForwarded(0, false)
		sns = r.subSNs(5)
		rtx, curbed = r.nack(sns[4])
		require.Equal(t, 1, rtx, "post-step packet keeps RTX while the key frame is pending")
		require.Zero(t, curbed)
	})

	t.Run("non-writable down track sends no PLI", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{keyFrame: true, layer: s0t2})
		r.preStep()
		require.NoError(t, r.dt.Unbind(nil))
		require.False(t, r.dt.IsWritableForTest(), "precondition: unbound")

		require.Zero(t, r.stepPLIs())
	})

	t.Run("flag on, a commit without a down-step sends no PLI", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{keyFrame: true, layer: s0t2})
		r.preStep()
		locked, _ := r.dt.ForwarderCheckSyncForTest()
		require.True(t, locked, "precondition: forwarder locked, the key frame requester stays quiet")

		before := r.dh.receiver.SendPLICallCount()
		alloc := r.commit(500_000, s0t2)
		require.False(t, alloc.IsDeficient)
		require.False(t, alloc.TemporalDownStep)
		require.Equal(t, before, r.dh.receiver.SendPLICallCount())
	})

	t.Run("flag off sends no PLI", func(t *testing.T) {
		r := newStepRig(t, stepRigOpts{keyFrame: false, layer: s0t2})
		r.preStep()
		require.True(t, r.dt.IsWritableForTest(), "precondition: writable")

		require.Zero(t, r.stepPLIs())
	})
}
