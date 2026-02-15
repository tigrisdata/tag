package auth

import (
	"context"
	"encoding/hex"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestDeriveGRPCAuthToken(t *testing.T) {
	token := DeriveGRPCAuthToken("my-access-key", "my-secret-key")

	// Should be a valid hex string
	decoded, err := hex.DecodeString(token)
	if err != nil {
		t.Fatalf("token %q is not valid hex: %v", token, err)
	}

	// HMAC-SHA256 produces 32 bytes = 64 hex chars
	if len(decoded) != 32 {
		t.Errorf("token decoded to %d bytes, want 32", len(decoded))
	}
}

func TestDeriveGRPCAuthToken_Deterministic(t *testing.T) {
	token1 := DeriveGRPCAuthToken("ak", "sk")
	token2 := DeriveGRPCAuthToken("ak", "sk")

	if token1 != token2 {
		t.Errorf("same inputs produced different tokens: %q vs %q", token1, token2)
	}
}

func TestDeriveGRPCAuthToken_DifferentInputs(t *testing.T) {
	token1 := DeriveGRPCAuthToken("ak1", "sk")
	token2 := DeriveGRPCAuthToken("ak2", "sk")
	token3 := DeriveGRPCAuthToken("ak1", "sk2")

	if token1 == token2 {
		t.Error("different access keys produced the same token")
	}
	if token1 == token3 {
		t.Error("different secret keys produced the same token")
	}
}

func TestValidateGRPCToken_Valid(t *testing.T) {
	token := "test-token-value"

	md := metadata.New(map[string]string{
		grpcAuthMetadataKey: token,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := validateGRPCToken(ctx, []byte(token))
	if err != nil {
		t.Fatalf("expected no error for valid token, got: %v", err)
	}
}

func TestValidateGRPCToken_Missing(t *testing.T) {
	ctx := context.Background() // no metadata

	err := validateGRPCToken(ctx, []byte("token"))
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestValidateGRPCToken_EmptyMetadata(t *testing.T) {
	md := metadata.New(map[string]string{})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := validateGRPCToken(ctx, []byte("token"))
	if err == nil {
		t.Fatal("expected error for missing token key")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestValidateGRPCToken_Invalid(t *testing.T) {
	md := metadata.New(map[string]string{
		grpcAuthMetadataKey: "wrong-token",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := validateGRPCToken(ctx, []byte("correct-token"))
	if err == nil {
		t.Fatal("expected error for invalid token")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestAttachGRPCToken(t *testing.T) {
	ctx := attachGRPCToken(context.Background(), "my-token")

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}

	values := md.Get(grpcAuthMetadataKey)
	if len(values) != 1 {
		t.Fatalf("expected 1 token value, got %d", len(values))
	}
	if values[0] != "my-token" {
		t.Errorf("token = %q, want %q", values[0], "my-token")
	}
}
