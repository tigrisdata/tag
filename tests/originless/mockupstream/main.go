// Command mockupstream is a minimal in-memory S3-ish origin for the origin-less
// lifecycle test's warm phase. It exists so the warm phase can populate a real
// TAG cache directory without credentials or a network upstream — a stand-in for
// the tigris gateway, which in production writes into the origin-less tier
// directly. When local writes land (phase 3), the warm phase becomes SDK PUTs
// against origin-less TAG itself and this mock is deleted.
//
// It implements exactly what the warm phase exercises: PUT stores bytes, GET and
// HEAD serve them with an ETag and Last-Modified, missing keys answer an S3
// NoSuchKey. Anonymous requests are allowed — the mock's 200 on an unsigned GET
// is what lets TAG cache the entry with the inferred public-read ACL, the only
// kind phase-1 origin-less serves.
package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type object struct {
	body []byte
	etag string
}

func main() {
	addr := ":18100"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	var mu sync.RWMutex
	store := map[string]object{}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			sum := md5.Sum(body)
			etag := `"` + hex.EncodeToString(sum[:]) + `"`
			mu.Lock()
			store[key] = object{body: body, etag: etag}
			mu.Unlock()
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet, http.MethodHead:
			mu.RLock()
			obj, ok := store[key]
			mu.RUnlock()
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist</Message><Resource>%s</Resource></Error>`, key)
				return
			}
			w.Header().Set("ETag", obj.etag)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(len(obj.body)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				w.Write(obj.body)
			}
		default:
			w.WriteHeader(http.StatusOK)
		}
	})

	fmt.Println("mock upstream listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
