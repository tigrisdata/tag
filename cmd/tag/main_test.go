package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// tagBinary returns the path to the pre-built TAG binary.
// Tests expect the binary to be built before running (e.g., via "make build").
func tagBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("TAG_TEST_BINARY")
	if binary == "" {
		binary = "../../tag"
	}
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		t.Skipf("TAG binary not found at %s; run 'make build' first", binary)
	}
	return binary
}

func TestVersionFlag(t *testing.T) {
	binary := tagBinary(t)

	out, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("tag --version failed: %v\n%s", err, out)
	}

	output := string(out)
	for _, expected := range []string{
		"TAG (Tigris Acceleration Gateway)",
		"Version:",
		"Build Time:",
		"Git Commit:",
	} {
		if !strings.Contains(output, expected) {
			t.Errorf("tag --version output missing %q\ngot: %s", expected, output)
		}
	}
}

func TestVersionFlag_SingleDash(t *testing.T) {
	binary := tagBinary(t)

	out, err := exec.Command(binary, "-version").CombinedOutput()
	if err != nil {
		t.Fatalf("tag -version failed: %v\n%s", err, out)
	}

	if !strings.Contains(string(out), "Version:") {
		t.Errorf("tag -version should work same as --version\ngot: %s", out)
	}
}

func TestHelpFlag(t *testing.T) {
	binary := tagBinary(t)

	// --help exits with code 2 via Go's flag package (flag.ErrHelp) with output to stderr
	cmd := exec.Command(binary, "--help")
	out, _ := cmd.CombinedOutput()

	output := string(out)
	for _, expected := range []string{
		"Usage: tag [options]",
		"--version",
		"--config",
		"--disable-cache",
		"--http-port",
		"--log-level",
		"--log-format",
		"TAG_HTTP_PORT",
		"TAG_LOG_LEVEL",
		"TAG_LOG_FORMAT",
	} {
		if !strings.Contains(output, expected) {
			t.Errorf("tag --help output missing %q\ngot: %s", expected, output)
		}
	}
}

func TestUnknownFlag(t *testing.T) {
	binary := tagBinary(t)

	cmd := exec.Command(binary, "--unknown-flag")
	err := cmd.Run()
	if err == nil {
		t.Error("tag --unknown-flag should exit with non-zero status")
	}
}
