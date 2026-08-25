package proxy

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	json "github.com/goccy/go-json"
	"github.com/tigrisdata/tag/cache"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/s3err"
)

// Origin-less bucket listing: ListObjects (V1) and ListObjectsV2 over the cached
// metadata, so callers — and people — can see what the tier holds.
//
// The listing is ADVISORY by contract: it reflects metadata presence at scan
// time. An entry mid-eviction can be listed and still answer NoSuchKey to the
// GET that follows; the read path's completeness gate stays the truth. That is
// the right trade for an observability listing — probing every listed entry's
// blocks would cost a page-sized fan-out per request.
//
// Ordering and pagination ride the storage: metadata keys are scanned in
// lexicographic order (S3's own ordering), the scan's continuation is
// last-key-exclusive (S3's marker semantics), and in cluster mode the serving
// node K-way merges all nodes, so results are complete and ordered cluster-wide.

const maxListKeys = 1000

// listParams carries one parsed listing request, either version.
type listParams struct {
	v2         bool
	prefix     string
	delimiter  string
	maxKeys    int
	encode     bool   // encoding-type=url
	after      string // resolved start point: marker (V1) / start-after or token (V2)
	tokenSent  bool   // V2: a continuation-token was supplied (echoed in response)
	lastPrefix string // V2 resume: last common prefix already emitted before this page
}

// v2 continuation tokens are opaque to clients: base64 of the last raw object
// key processed plus the last common prefix already emitted, so a page boundary
// inside a delimiter group neither re-emits the group nor skips its successor.
type listCursor struct {
	AfterKey   string `json:"k"`
	LastPrefix string `json:"p"`
}

func encodeListToken(c listCursor) string {
	b, _ := json.Marshal(c)
	return base64.URLEncoding.EncodeToString(b)
}

func decodeListToken(s string) (listCursor, bool) {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return listCursor{}, false
	}
	var c listCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return listCursor{}, false
	}
	return c, true
}

// s3URLEncode applies S3's encoding-type=url: percent-encode everything outside
// the unreserved set, space included (%20, never "+").
func s3URLEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

type listXMLObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listXMLPrefix struct {
	Prefix string `xml:"Prefix"`
}

type listXMLResultV1 struct {
	XMLName        xml.Name        `xml:"ListBucketResult"`
	Xmlns          string          `xml:"xmlns,attr"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Marker         string          `xml:"Marker"`
	NextMarker     string          `xml:"NextMarker,omitempty"`
	MaxKeys        int             `xml:"MaxKeys"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	EncodingType   string          `xml:"EncodingType,omitempty"`
	IsTruncated    bool            `xml:"IsTruncated"`
	Contents       []listXMLObject `xml:"Contents"`
	CommonPrefixes []listXMLPrefix `xml:"CommonPrefixes"`
}

type listXMLResultV2 struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Xmlns                 string          `xml:"xmlns,attr"`
	Name                  string          `xml:"Name"`
	Prefix                string          `xml:"Prefix"`
	StartAfter            string          `xml:"StartAfter,omitempty"`
	ContinuationToken     string          `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
	KeyCount              int             `xml:"KeyCount"`
	MaxKeys               int             `xml:"MaxKeys"`
	Delimiter             string          `xml:"Delimiter,omitempty"`
	EncodingType          string          `xml:"EncodingType,omitempty"`
	IsTruncated           bool            `xml:"IsTruncated"`
	Contents              []listXMLObject `xml:"Contents"`
	CommonPrefixes        []listXMLPrefix `xml:"CommonPrefixes"`
}

// parseListParams validates the query. Unknown parameters answer 501 (nil, false)
// — same contract as everywhere else in the mode: refuse rather than half-honor.
func parseListParams(r *http.Request) (*listParams, bool) {
	q := r.URL.Query()
	p := &listParams{maxKeys: maxListKeys}
	p.v2 = q.Get("list-type") == "2"

	allowed := map[string]bool{
		"list-type": true, "prefix": true, "delimiter": true, "max-keys": true,
		"encoding-type": true, "x-id": true, "fetch-owner": true,
	}
	for _, p := range originlessIgnoredParams {
		allowed[p] = true // presigned listings are served like header-signed ones
	}
	if p.v2 {
		allowed["continuation-token"] = true
		allowed["start-after"] = true
	} else {
		allowed["marker"] = true
	}
	for name := range q {
		if !allowed[name] {
			return nil, false
		}
	}

	p.prefix = q.Get("prefix")
	p.delimiter = q.Get("delimiter")
	if enc := q.Get("encoding-type"); enc != "" {
		if enc != "url" {
			return nil, false
		}
		p.encode = true
	}
	if mk := q.Get("max-keys"); mk != "" {
		n, err := strconv.Atoi(mk)
		if err != nil || n < 0 {
			return nil, false
		}
		if n < p.maxKeys {
			p.maxKeys = n
		}
	}
	if p.v2 {
		p.after = q.Get("start-after")
		if tok := q.Get("continuation-token"); tok != "" {
			p.tokenSent = true
			c, ok := decodeListToken(tok)
			if !ok {
				return nil, false
			}
			p.after = c.AfterKey
			// The emitted-prefix guard rides in the token; stash it via after-key
			// handling below (cursor recreated in the walk).
			p.lastPrefix = c.LastPrefix
		}
	} else {
		p.after = q.Get("marker")
		// V1 has no token to carry emitted-group state across pages. When the
		// client resumes from a NextMarker that names a delimiter group (S3's V1
		// convention), every member of that group sorts after the marker and the
		// group would be re-emitted. Seeding the guard with the marker makes any
		// group at-or-before it skip, which is exactly S3's resume semantics.
		if p.delimiter != "" && p.after != "" {
			p.lastPrefix = p.after
		}
	}
	return p, true
}

// HandleOriginlessList serves ListObjects / ListObjectsV2 from cached metadata.
func (s *Service) HandleOriginlessList(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, _ := ParseBucketKey(r)

	p, ok := parseListParams(r)
	if !ok {
		return s.HandleOriginlessUnsupported(w, r)
	}
	if !s.cache.IsEnabled() {
		s3err.WriteError(w, r, s3err.ErrNotImplemented)
		metrics.RecordRequest("ListObjects", "unsupported", time.Since(start).Seconds())
		return nil
	}

	contents, prefixes, truncated, nextCursor, err := s.walkListing(r, bucket, p)
	if err != nil {
		metrics.RecordRequest("ListObjects", "error", time.Since(start).Seconds())
		return err
	}

	enc := func(v string) string {
		if p.encode {
			return s3URLEncode(v)
		}
		return v
	}
	xmlContents := make([]listXMLObject, 0, len(contents))
	for _, e := range contents {
		xmlContents = append(xmlContents, listXMLObject{
			Key:          enc(e.Key),
			LastModified: time.Unix(e.Meta.LastModified, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         e.Meta.ETag,
			Size:         e.Meta.ContentLength,
			StorageClass: "STANDARD",
		})
	}
	xmlPrefixes := make([]listXMLPrefix, 0, len(prefixes))
	for _, cp := range prefixes {
		xmlPrefixes = append(xmlPrefixes, listXMLPrefix{Prefix: enc(cp)})
	}
	encodingType := ""
	if p.encode {
		encodingType = "url"
	}

	var body any
	if p.v2 {
		res := listXMLResultV2{
			Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket,
			Prefix: enc(p.prefix), Delimiter: enc(p.delimiter), EncodingType: encodingType,
			MaxKeys: p.maxKeys, IsTruncated: truncated,
			KeyCount: len(contents) + len(prefixes),
			Contents: xmlContents, CommonPrefixes: xmlPrefixes,
		}
		if q := r.URL.Query().Get("start-after"); q != "" {
			res.StartAfter = enc(q)
		}
		if p.tokenSent {
			res.ContinuationToken = r.URL.Query().Get("continuation-token")
		}
		if truncated {
			res.NextContinuationToken = encodeListToken(nextCursor)
		}
		body = res
	} else {
		res := listXMLResultV1{
			Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket,
			// V1 echoes Prefix RAW even under encoding-type=url: botocore's V1
			// decoder handles Marker/Delimiter/Keys/CommonPrefixes but not the
			// top-level Prefix, so an encoded one reaches clients double-encoded.
			// (V2's decoder does handle Prefix; it stays encoded there.)
			Prefix: p.prefix, Delimiter: enc(p.delimiter), EncodingType: encodingType,
			Marker: enc(r.URL.Query().Get("marker")), MaxKeys: p.maxKeys, IsTruncated: truncated,
			Contents: xmlContents, CommonPrefixes: xmlPrefixes,
		}
		if truncated && p.delimiter != "" {
			// V1 returns NextMarker only when a delimiter is in play; otherwise the
			// client continues from the last Contents key.
			res.NextMarker = enc(lastItem(contents, prefixes))
		}
		body = res
	}

	out, err := xml.Marshal(body)
	if err != nil {
		metrics.RecordRequest("ListObjects", "error", time.Since(start).Seconds())
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	w.Write(out)
	metrics.RecordRequest("ListObjects", "success", time.Since(start).Seconds())
	return nil
}

// walkListing pages the metadata scan, applying delimiter rollup and truncation.
// Both keys and rolled-up common prefixes count toward maxKeys, as S3 counts
// them. nextCursor records the last raw key processed plus the last common
// prefix emitted, so a resume mid-group neither repeats nor skips.
func (s *Service) walkListing(r *http.Request, bucket string, p *listParams) (
	contents []cache.ListedEntry, prefixes []string, truncated bool, nextCursor listCursor, err error,
) {
	if p.maxKeys == 0 {
		return nil, nil, false, listCursor{}, nil
	}
	after := p.after
	prev := p.after
	lastEmittedPrefix := p.lastPrefix
	items := 0

	for {
		page, nextAfter, hasMore, lerr := s.cache.ListMeta(r.Context(), bucket, p.prefix, after, maxListKeys)
		if lerr != nil {
			return nil, nil, false, listCursor{}, lerr
		}
		for _, e := range page {
			if p.delimiter != "" {
				rest := strings.TrimPrefix(e.Key, p.prefix)
				if idx := strings.Index(rest, p.delimiter); idx >= 0 {
					cp := p.prefix + rest[:idx+len(p.delimiter)]
					if cp <= lastEmittedPrefix {
						prev = e.Key
						continue // member of a group already emitted
					}
					if items == p.maxKeys {
						// The overflowing item must be re-seen on resume: the scan is
						// exclusive, so the cursor is the key BEFORE it.
						return contents, prefixes, true, listCursor{AfterKey: prev, LastPrefix: lastEmittedPrefix}, nil
					}
					prefixes = append(prefixes, cp)
					lastEmittedPrefix = cp
					items++
					prev = e.Key
					continue
				}
			}
			if items == p.maxKeys {
				return contents, prefixes, true, listCursor{AfterKey: prev, LastPrefix: lastEmittedPrefix}, nil
			}
			contents = append(contents, e)
			items++
			prev = e.Key
		}
		// Advance by the RAW scan cursor, not the surviving entries: a page whose
		// entries were all skipped must still move forward or the walk spins on
		// one token until the server timeout.
		if nextAfter != "" {
			after = nextAfter
		} else if hasMore {
			return contents, prefixes, false, listCursor{}, fmt.Errorf("listing scan returned no cursor with more pages claimed")
		}
		if !hasMore {
			return contents, prefixes, false, listCursor{}, nil
		}
	}
}

func lastItem(contents []cache.ListedEntry, prefixes []string) string {
	last := ""
	if len(contents) > 0 {
		last = contents[len(contents)-1].Key
	}
	if len(prefixes) > 0 && prefixes[len(prefixes)-1] > last {
		last = prefixes[len(prefixes)-1]
	}
	return last
}
