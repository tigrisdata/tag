package proxy

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

func TestSignForwardPreservesRequestTarget(t *testing.T) {
	for _, target := range []struct {
		path, query string
	}{
		{"/bucket/key", ""},
		{"/bucket/key", "list-type=2&prefix=a%2Fb"},
		{"/bucket/key", "x=a?b"},
		{"/bucket/key", "x=a#b"},
		{"/bucket/key%2Fwith space+and%25", "x=a%23b"},
		{"/bucket/key?literal", ""},
		{"/bucket/key?literal", "x=a?b#c"},
	} {
		t.Run(target.path+"?"+target.query, func(t *testing.T) {
			f := signingForwarder{baseForwarder: newBaseForwarder("https://upstream.example.com", "us-east-1", 1)}
			r := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: target.path, RawQuery: target.query}}
			got, err := f.signForward(t.Context(), r, nil, "", 0, false, "AK", "SK")
			if err != nil {
				t.Fatal(err)
			}
			path := target.path
			if target.query != "" {
				path += "?" + target.query
			}
			want, err := f.signer.SignRequest(t.Context(), r.Method, path, nil, "", "AK", "SK", nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.URL, want.URL) || got.Host != want.Host {
				t.Fatalf("target = %#v, host %q; want %#v, host %q", got.URL, got.Host, want.URL, want.Host)
			}
		})
	}
}
