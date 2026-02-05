package sdk

import (
	"fmt"
	"os"
	"testing"
)

// globalEnv is the shared test environment for all SDK tests.
var globalEnv *TestEnvironment

// TestMain sets up and tears down the test environment.
func TestMain(m *testing.M) {
	// Try to create test environment
	env, err := NewTestEnvironment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Skipping SDK tests: %v\n", err)
		fmt.Fprintf(os.Stderr, "To run SDK tests:\n")
		fmt.Fprintf(os.Stderr, "  1. Set credentials: export AWS_ACCESS_KEY_ID=<key> AWS_SECRET_ACCESS_KEY=<secret>\n")
		fmt.Fprintf(os.Stderr, "  2. Start TAG: make s3-test-local\n")
		fmt.Fprintf(os.Stderr, "  3. Run tests: make test-sdk\n")
		os.Exit(0) // Skip gracefully, not failure
	}
	globalEnv = env

	fmt.Printf("SDK tests using TAG at %s with bucket prefix %s\n", env.Endpoint, env.BucketPrefix)

	// Run tests
	code := m.Run()

	// Cleanup test buckets
	if err := env.Cleanup(); err != nil {
		fmt.Fprintf(os.Stderr, "Cleanup warning: %v\n", err)
	}

	os.Exit(code)
}
