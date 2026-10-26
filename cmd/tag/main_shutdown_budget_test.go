// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"
)

// The drain window plus the cache's leave reserve must fit in the base
// manifests' termination grace, or a busy pod is SIGKILLed before it has left
// the gossip cluster and peers keep routing to a dead address.
func TestShutdownDrainLeavesRoomForClusterLeave(t *testing.T) {
	if shutdownTimeout+clusterLeaveReserve > baseTerminationGrace {
		t.Fatalf("drain %s + leave reserve %s exceeds termination grace %s", shutdownTimeout, clusterLeaveReserve, baseTerminationGrace)
	}
	if clusterLeaveReserve < 10*time.Second {
		t.Fatalf("leave reserve %s is below the 5s memberlist leave timeout plus gRPC stop and storage close", clusterLeaveReserve)
	}
}
