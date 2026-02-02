package integration

import (
	"fmt"
	"os"
	"testing"
)

// TestMain sets up the shared embedded cache for integration tests.
func TestMain(m *testing.M) {
	// Set up the shared embedded cache
	if err := setupSharedCache(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup shared cache: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Clean up
	teardownSharedCache()

	os.Exit(code)
}
