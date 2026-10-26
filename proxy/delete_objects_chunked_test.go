package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type chunkedDeleteForwardMode uint8

const (
	chunkedDeleteTransparent chunkedDeleteForwardMode = iota
	chunkedDeleteSigning
)

type chunkedDeleteCase struct {
	name            string
	mode            chunkedDeleteForwardMode
	tiered          bool
	key             string
	body            []byte
	chunks          [][]byte
	responseStatus  int
	responseBody    []byte
	responseHeader  string
	upstreamAccepts bool
	repopulate      bool
	preCached       bool
	finalCached     bool
	finalETag       string
}

type chunkedDeleteObservation struct {
	requestSeen     bool
	method          string
	path            string
	hasDeleteQuery  bool
	contentLength   int64
	authorization   string
	contentHash     string
	decodedLength   string
	contentEncoding string
	trailerHeader   string
	wireBody        []byte
	decodedBody     []byte
	trailer         []byte
	authErr         error
	readErr         error
	decodeErr       error
	checksumValid   bool
	xmlErr          error
	keyMatches      bool
	accepted        bool
	preCached       bool
	cacheReadErr    error
	refillErr       error
}

func bulkDeleteXML(key string, quiet bool) []byte {
	body := `<Delete><Object><Key>` + key + `</Key></Object>`
	if quiet {
		body += `<Quiet>true</Quiet>`
	}
	return []byte(body + `</Delete>`)
}

func splitChunkAt(body []byte, index int) [][]byte {
	return [][]byte{bytes.Clone(body[:index]), bytes.Clone(body[index:])}
}

func unsignedTrailerChecksum(body []byte) []byte {
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.ChecksumIEEE(body))
	return []byte("x-amz-checksum-crc32:" + base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n")
}

func unsignedTrailerWire(body []byte, chunks [][]byte) []byte {
	var wire bytes.Buffer
	for _, chunk := range chunks {
		fmt.Fprintf(&wire, "%x\r\n", len(chunk))
		wire.Write(chunk)
		wire.WriteString("\r\n")
	}
	wire.WriteString("0\r\n")
	wire.Write(unsignedTrailerChecksum(body))
	return wire.Bytes()
}

func decodeUnsignedTrailerWire(wire []byte) (decoded, trailer []byte, err error) {
	reader := newAWSChunkedReader(bytes.NewReader(wire))
	decoded, err = io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}
	trailer, err = io.ReadAll(reader.reader)
	return decoded, trailer, err
}

func newSignedUnsignedTrailerDelete(t *testing.T, endpoint, path string, body []byte, chunks [][]byte) (*http.Request, []byte) {
	t.Helper()
	if !bytes.Equal(bytes.Join(chunks, nil), body) {
		t.Fatalf("test setup: chunks do not reconstruct the decoded DeleteObjects body")
	}
	wire := unsignedTrailerWire(body, chunks)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+path, io.NopCloser(bytes.NewReader(wire)))
	if err != nil {
		t.Fatalf("test setup: create signed DeleteObjects request: %v", err)
	}
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("X-Amz-Content-Sha256", StreamingUnsignedTrailerHash)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(body)))
	req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
	creds := aws.Credentials{AccessKeyID: chunkVerifyAccessKey, SecretAccessKey: chunkVerifySecretKey}
	if err := v4.NewSigner().SignHTTP(t.Context(), creds, req, StreamingUnsignedTrailerHash, "s3", chunkVerifyRegion, time.Now().UTC().Truncate(time.Second)); err != nil {
		t.Fatalf("test setup: sign DeleteObjects request: %v", err)
	}
	req.ContentLength = int64(len(wire))
	return req, wire
}

func runChunkedDeleteCase(t *testing.T, tc chunkedDeleteCase) {
	t.Helper()
	const bucket = "bulk-bucket"
	path := "/" + bucket + "?delete"
	wire := unsignedTrailerWire(tc.body, tc.chunks)
	if !bytes.Equal(bytes.Join(tc.chunks, nil), tc.body) {
		t.Fatalf("test setup: chunks do not reconstruct the decoded DeleteObjects body")
	}
	if decoded, trailer, err := decodeUnsignedTrailerWire(wire); err != nil || !bytes.Equal(decoded, tc.body) || !bytes.Equal(trailer, unsignedTrailerChecksum(tc.body)) {
		t.Fatalf("test setup: local upstream fixture does not accept the framed request: decoded=%q trailer=%q err=%v", decoded, trailer, err)
	}

	cfg := config.NewDefault()
	cfg.Upstream.Region = chunkVerifyRegion
	if tc.tiered {
		cfg.Mode = config.ModeTiered
		cfg.Cache.SetBlockCachingEnabled(false)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("test setup: configure tiered mode: %v", err)
		}
	}
	cacheStore := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	if !cacheStore.IsEnabled() || cfg.IsTiered() != tc.tiered {
		t.Fatalf("test setup: cache enabled/tiered mode = %t/%t, want true/%t", cacheStore.IsEnabled(), cfg.IsTiered(), tc.tiered)
	}
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(chunkVerifyAccessKey, chunkVerifySecretKey)
	validator := auth.NewRequestValidator(credentials)
	observations := make(chan chunkedDeleteObservation, 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs := chunkedDeleteObservation{
			requestSeen:     true,
			method:          r.Method,
			path:            r.URL.Path,
			hasDeleteQuery:  r.URL.Query().Has("delete"),
			contentLength:   r.ContentLength,
			authorization:   r.Header.Get("Authorization"),
			contentHash:     r.Header.Get("X-Amz-Content-Sha256"),
			decodedLength:   r.Header.Get("X-Amz-Decoded-Content-Length"),
			contentEncoding: r.Header.Get("Content-Encoding"),
			trailerHeader:   r.Header.Get("X-Amz-Trailer"),
		}
		_, obs.authErr = validator.Validate(r)
		obs.wireBody, obs.readErr = io.ReadAll(r.Body)
		_ = r.Body.Close()

		if tc.mode == chunkedDeleteTransparent {
			obs.decodedBody, obs.trailer, obs.decodeErr = decodeUnsignedTrailerWire(obs.wireBody)
			obs.checksumValid = obs.decodeErr == nil &&
				obs.trailerHeader == "x-amz-checksum-crc32" &&
				bytes.Equal(obs.trailer, unsignedTrailerChecksum(tc.body))
		} else {
			obs.decodedBody = bytes.Clone(obs.wireBody)
			obs.checksumValid = obs.contentHash == "UNSIGNED-PAYLOAD" &&
				obs.decodedLength == "" && obs.contentEncoding == ""
		}

		var deleteReq deleteObjectsRequest
		if obs.readErr == nil && obs.decodeErr == nil {
			obs.xmlErr = xml.Unmarshal(obs.decodedBody, &deleteReq)
		}
		if obs.xmlErr == nil {
			obs.keyMatches = len(deleteReq.Objects) == 1 &&
				deleteReq.Objects[0].Key == tc.key &&
				deleteReq.Quiet == bytes.Contains(tc.body, []byte(`<Quiet>true</Quiet>`))
		}
		bodyMatches := bytes.Equal(obs.decodedBody, tc.body)
		if tc.mode == chunkedDeleteTransparent {
			bodyMatches = bodyMatches && bytes.Equal(obs.wireBody, wire) &&
				obs.contentHash == StreamingUnsignedTrailerHash &&
				obs.decodedLength == strconv.Itoa(len(tc.body)) &&
				obs.contentEncoding == "aws-chunked" &&
				obs.contentLength == int64(len(wire))
		} else {
			bodyMatches = bodyMatches && obs.contentLength == int64(len(tc.body))
		}
		obs.accepted = obs.method == http.MethodPost && obs.path == "/"+bucket &&
			obs.hasDeleteQuery && obs.authErr == nil && obs.readErr == nil &&
			obs.decodeErr == nil && obs.checksumValid && bodyMatches && obs.xmlErr == nil && obs.keyMatches

		_, obs.preCached, obs.cacheReadErr = cacheStore.GetMeta(context.Background(), bucket, tc.key)
		if obs.accepted && tc.repopulate {
			refill := &cache.CachedObjectMeta{Bucket: bucket, Key: tc.key, ETag: `"racing-refill"`, ContentLength: 6, StatusCode: http.StatusOK}
			obs.refillErr = cacheStore.PutWithMeta(context.Background(), bucket, tc.key, refill, []byte("refill"), 60)
		}

		status, responseBody, result := tc.responseStatus, tc.responseBody, tc.responseHeader
		if obs.accepted != tc.upstreamAccepts {
			status = http.StatusBadRequest
			responseBody = []byte(`<Error><Code>MalformedXML</Code></Error>`)
			result = "rejected"
		}
		observations <- obs
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Content-Length", strconv.Itoa(len(responseBody)))
		w.Header().Set("X-Upstream-Delete-Result", result)
		w.WriteHeader(status)
		_, _ = w.Write(responseBody)
	}))
	defer upstream.Close()

	cfg.Upstream.Endpoint = upstream.URL
	var forwarder RequestForwarder
	if tc.mode == chunkedDeleteTransparent {
		forwarder = NewForwarder(credentials, upstream.URL, cfg.Upstream.Region, 1, auth.NewProxySigner("proxy-access", "proxy-secret"), nil)
	} else {
		forwarder = NewForwarder(credentials, upstream.URL, cfg.Upstream.Region, 1, nil, nil)
	}
	switch fwd := forwarder.(type) {
	case *transparentForwarder:
		fwd.baseForwarder.httpClient = upstream.Client()
	case *signingForwarder:
		fwd.baseForwarder.httpClient = upstream.Client()
	}
	service := NewService(forwarder, cacheStore, cfg)
	request, requestWire := newSignedUnsignedTrailerDelete(t, upstream.URL, path, tc.body, tc.chunks)
	originalAuthorization := request.Header.Get("Authorization")
	if _, err := validator.Validate(request); err != nil {
		t.Fatalf("test setup: request signature is invalid: %v", err)
	}
	if !bytes.Equal(requestWire, wire) {
		t.Fatalf("test setup: request and upstream fixtures disagree on wire bytes")
	}

	initial := &cache.CachedObjectMeta{Bucket: bucket, Key: tc.key, ETag: `"initial"`, ContentLength: 3, StatusCode: http.StatusOK}
	if err := cacheStore.PutWithMeta(context.Background(), bucket, tc.key, initial, []byte("old"), 60); err != nil {
		t.Fatalf("test setup: seed cache entry: %v", err)
	}

	writer := httptest.NewRecorder()
	handlerErr := service.HandleDeleteObjects(writer, request)
	var obs chunkedDeleteObservation
	select {
	case obs = <-observations:
	default:
	}

	var failures []string
	check := func(ok bool, detail string) {
		if !ok {
			failures = append(failures, detail)
		}
	}
	check(handlerErr == nil, fmt.Sprintf("handler returned %v", handlerErr))
	check(obs.requestSeen, "the configured upstream did not receive the request")
	check(obs.accepted == tc.upstreamAccepts, fmt.Sprintf("upstream acceptance = %t, want %t", obs.accepted, tc.upstreamAccepts))
	check(obs.authErr == nil, fmt.Sprintf("forwarded request signature was rejected: %v", obs.authErr))
	check(obs.readErr == nil && obs.decodeErr == nil, fmt.Sprintf("forwarded body read/decode errors = %v/%v", obs.readErr, obs.decodeErr))
	check(obs.checksumValid, "forwarded request framing/checksum did not match the accepted payload")
	check(bytes.Equal(obs.decodedBody, tc.body), "upstream-decoded request body differs from the client XML")
	check((obs.xmlErr == nil) == tc.upstreamAccepts, fmt.Sprintf("upstream XML parse error = %v, accepted=%t", obs.xmlErr, tc.upstreamAccepts))
	check(obs.cacheReadErr == nil && obs.refillErr == nil, fmt.Sprintf("cache observation/refill errors = %v/%v", obs.cacheReadErr, obs.refillErr))
	check(obs.preCached == tc.preCached, fmt.Sprintf("cache entry before upstream = %t, want %t", obs.preCached, tc.preCached))
	check(writer.Code == tc.responseStatus, fmt.Sprintf("client status = %d, want %d", writer.Code, tc.responseStatus))
	check(writer.Header().Get("X-Upstream-Delete-Result") == tc.responseHeader, fmt.Sprintf("client result header = %q, want %q", writer.Header().Get("X-Upstream-Delete-Result"), tc.responseHeader))
	check(bytes.Equal(writer.Body.Bytes(), tc.responseBody), "client response body differs from the configured upstream response")
	check(request.Header.Get("Authorization") == originalAuthorization, "the original request Authorization header changed")
	if tc.mode == chunkedDeleteTransparent {
		check(bytes.Equal(obs.wireBody, requestWire), "transparent forwarding changed the AWS-chunked wire body")
		check(obs.authorization == originalAuthorization, "transparent forwarding changed the signed Authorization header")
		check(obs.contentHash == StreamingUnsignedTrailerHash && obs.decodedLength == strconv.Itoa(len(tc.body)) && obs.contentEncoding == "aws-chunked" && obs.trailerHeader == "x-amz-checksum-crc32", "transparent forwarding changed AWS streaming headers")
	} else {
		check(bytes.Equal(obs.wireBody, tc.body), "signing forwarding did not send the decoded XML body")
		check(obs.authorization != originalAuthorization, "signing forwarding did not re-sign the decoded request")
		check(obs.contentHash == "UNSIGNED-PAYLOAD" && obs.decodedLength == "" && obs.contentEncoding == "", "signing forwarding retained AWS chunk-framing headers")
	}
	finalMeta, finalCached, cacheErr := cacheStore.GetMeta(context.Background(), bucket, tc.key)
	check(cacheErr == nil, fmt.Sprintf("final cache read error = %v", cacheErr))
	check(finalCached == tc.finalCached, fmt.Sprintf("final cache presence = %t, want %t", finalCached, tc.finalCached))
	if tc.finalCached && finalMeta != nil {
		check(finalMeta.ETag == tc.finalETag, fmt.Sprintf("final cache ETag = %q, want %q", finalMeta.ETag, tc.finalETag))
	}
	if len(failures) != 0 {
		t.Errorf("AWS-chunked DeleteObjects did not preserve decoded-key invalidation and forwarding behavior: %s", strings.Join(failures, "; "))
	}
}

func TestHandleDeleteObjectsAWSChunkedRequest(t *testing.T) {
	tagSplitXML := bulkDeleteXML("tag-split-key", true)
	tagSplitAt := bytes.Index(tagSplitXML, []byte("<Key>")) + len("<Ke")
	textSplitXML := bulkDeleteXML("text-split-key", true)
	textSplitAt := bytes.Index(textSplitXML, []byte("text-split-key")) + len("text-split")
	singleChunkXML := bulkDeleteXML("single-chunk-key", true)
	failedXML := bulkDeleteXML("failed-chunk-key", false)
	malformedXML := []byte(`<Delete><Object><Key>malformed-chunk-key</Key></Object>`)
	tieredXML := bulkDeleteXML("tiered-split-key", true)
	tieredSplitAt := bytes.Index(tieredXML, []byte("<Key>")) + len("<Ke")
	tieredFailedXML := bulkDeleteXML("tiered-failed-key", false)
	tieredFailedSplitAt := bytes.Index(tieredFailedXML, []byte("<Key>")) + len("<Ke")

	cases := []chunkedDeleteCase{
		{
			name: "transparent/tag-split-quiet-success", mode: chunkedDeleteTransparent,
			key: "tag-split-key", body: tagSplitXML, chunks: splitChunkAt(tagSplitXML, tagSplitAt),
			responseStatus: http.StatusOK, responseBody: []byte(`<DeleteResult/>`), responseHeader: "quiet-success",
			upstreamAccepts: true, repopulate: true, finalCached: false,
		},
		{
			name: "transparent/key-text-split-quiet-success", mode: chunkedDeleteTransparent,
			key: "text-split-key", body: textSplitXML, chunks: splitChunkAt(textSplitXML, textSplitAt),
			responseStatus: http.StatusOK, responseBody: []byte(`<DeleteResult/>`), responseHeader: "quiet-success",
			upstreamAccepts: true, repopulate: true, finalCached: false,
		},
		{
			name: "transparent/single-chunk-control", mode: chunkedDeleteTransparent,
			key: "single-chunk-key", body: singleChunkXML, chunks: [][]byte{singleChunkXML},
			responseStatus: http.StatusOK, responseBody: []byte(`<DeleteResult/>`), responseHeader: "single-chunk",
			upstreamAccepts: true, repopulate: true, finalCached: false,
		},
		{
			name: "transparent/complete-per-key-failure", mode: chunkedDeleteTransparent,
			key: "failed-chunk-key", body: failedXML, chunks: [][]byte{failedXML},
			responseStatus: http.StatusOK,
			responseBody:   []byte(`<DeleteResult><Error><Key>failed-chunk-key</Key><Code>AccessDenied</Code></Error></DeleteResult>`),
			responseHeader: "per-key-failure", upstreamAccepts: true, repopulate: true, finalCached: true, finalETag: `"racing-refill"`,
		},
		{
			name: "transparent/malformed-decoded-xml", mode: chunkedDeleteTransparent,
			key: "malformed-chunk-key", body: malformedXML, chunks: [][]byte{malformedXML},
			responseStatus: http.StatusBadRequest, responseBody: []byte(`<Error><Code>MalformedXML</Code></Error>`),
			responseHeader: "malformed-xml", upstreamAccepts: false, preCached: true, finalCached: true, finalETag: `"initial"`,
		},
		{
			name: "signing/tag-split", mode: chunkedDeleteSigning,
			key: "tag-split-key", body: tagSplitXML, chunks: splitChunkAt(tagSplitXML, tagSplitAt),
			responseStatus: http.StatusOK, responseBody: []byte(`<DeleteResult/>`), responseHeader: "signing-success",
			upstreamAccepts: true, repopulate: true, finalCached: false,
		},
		{
			name: "tiered/tag-split-success", mode: chunkedDeleteTransparent, tiered: true,
			key: "tiered-split-key", body: tieredXML, chunks: splitChunkAt(tieredXML, tieredSplitAt),
			responseStatus: http.StatusOK, responseBody: []byte(`<DeleteResult/>`), responseHeader: "tiered-success",
			upstreamAccepts: true, preCached: true, finalCached: false,
		},
		{
			name: "tiered/tag-split-all-failed", mode: chunkedDeleteTransparent, tiered: true,
			key: "tiered-failed-key", body: tieredFailedXML, chunks: splitChunkAt(tieredFailedXML, tieredFailedSplitAt),
			responseStatus: http.StatusOK,
			responseBody:   []byte(`<DeleteResult><Error><Key>tiered-failed-key</Key><Code>AccessDenied</Code></Error></DeleteResult>`),
			responseHeader: "tiered-failure", upstreamAccepts: true, preCached: true, finalCached: true, finalETag: `"initial"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runChunkedDeleteCase(t, tc) })
	}
}
