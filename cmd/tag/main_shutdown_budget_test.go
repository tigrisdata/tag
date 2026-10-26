// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// The drain window plus the cache's leave reserve must fit in the termination
// grace the base manifests actually set, or a busy pod is SIGKILLed before it
// has left the gossip cluster and peers keep routing to a dead address.
func TestShutdownDrainLeavesRoomForClusterLeave(t *testing.T) {
	manifest, err := os.ReadFile("../../deploy/kubernetes/base/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*terminationGracePeriodSeconds:\s*(\d+)\s*$`).FindSubmatch(manifest)
	if m == nil {
		t.Fatal("base statefulset sets no terminationGracePeriodSeconds")
	}
	seconds, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}
	manifestGrace := time.Duration(seconds) * time.Second

	if baseTerminationGrace != manifestGrace {
		t.Fatalf("baseTerminationGrace %s does not match the base manifest's %s", baseTerminationGrace, manifestGrace)
	}
	if shutdownTimeout+clusterLeaveReserve > manifestGrace {
		t.Fatalf("drain %s + leave reserve %s exceeds the manifest termination grace %s", shutdownTimeout, clusterLeaveReserve, manifestGrace)
	}
	if clusterLeaveReserve < 10*time.Second {
		t.Fatalf("leave reserve %s is below the 5s memberlist leave timeout plus gRPC stop and storage close", clusterLeaveReserve)
	}
}
