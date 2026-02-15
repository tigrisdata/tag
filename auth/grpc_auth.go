package auth

import (
	"context"
	"crypto/subtle"
	"encoding/hex"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// grpcAuthMetadataKey is the gRPC metadata key for the cache auth token.
	grpcAuthMetadataKey = "x-cache-auth-token"

	// grpcAuthDerivationInput is the HMAC input used to derive the auth token.
	grpcAuthDerivationInput = "ocache-grpc-auth-v1"
)

// DeriveGRPCAuthToken derives a shared authentication token from the proxy's
// access key and secret key. All nodes in the cluster must use the same
// credentials to derive matching tokens.
//
// The token is HMAC-SHA256(secretKey, "ocache-grpc-auth-v1:"+accessKey),
// encoded as hex.
func DeriveGRPCAuthToken(accessKey, secretKey string) string {
	input := grpcAuthDerivationInput + ":" + accessKey
	mac := hmacSHA256([]byte(secretKey), []byte(input))
	return hex.EncodeToString(mac)
}

// GRPCServerOptions returns gRPC server options that enforce auth token
// validation on both unary and streaming RPCs.
func GRPCServerOptions(token string) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcUnaryServerInterceptor(token)),
		grpc.StreamInterceptor(grpcStreamServerInterceptor(token)),
	}
}

// GRPCDialOptions returns gRPC dial options that attach the auth token
// to both unary and streaming RPCs.
func GRPCDialOptions(token string) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpcUnaryClientInterceptor(token)),
		grpc.WithStreamInterceptor(grpcStreamClientInterceptor(token)),
	}
}

// grpcUnaryServerInterceptor returns a gRPC unary server interceptor that
// validates the auth token from incoming metadata.
func grpcUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	tokenBytes := []byte(token)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := validateGRPCToken(ctx, tokenBytes); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// grpcStreamServerInterceptor returns a gRPC stream server interceptor that
// validates the auth token from incoming metadata.
func grpcStreamServerInterceptor(token string) grpc.StreamServerInterceptor {
	tokenBytes := []byte(token)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateGRPCToken(ss.Context(), tokenBytes); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// grpcUnaryClientInterceptor returns a gRPC unary client interceptor that
// attaches the auth token to outgoing metadata.
func grpcUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = attachGRPCToken(ctx, token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// grpcStreamClientInterceptor returns a gRPC stream client interceptor that
// attaches the auth token to outgoing metadata.
func grpcStreamClientInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = attachGRPCToken(ctx, token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// validateGRPCToken extracts and validates the auth token from incoming gRPC metadata.
func validateGRPCToken(ctx context.Context, expectedToken []byte) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get(grpcAuthMetadataKey)
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "missing auth token")
	}

	if subtle.ConstantTimeCompare([]byte(values[0]), expectedToken) != 1 {
		return status.Error(codes.Unauthenticated, "invalid auth token")
	}

	return nil
}

// attachGRPCToken attaches the auth token to outgoing gRPC metadata.
func attachGRPCToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, grpcAuthMetadataKey, token)
}
