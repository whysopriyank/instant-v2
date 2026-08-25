package platform

import "testing"

// TestOriginOf pins redirect-origin extraction: only http(s) with a host
// yields an origin; host material with control characters or whitespace is
// rejected so nothing hostile can ride into a Location header.
func TestOriginOf(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"http://app.example/cb?x=1", "http://app.example", true},
		{"HTTPS://APP.example:8443/cb", "https://app.example:8443", true},
		{"ftp://app.example/cb", "", false},
		{"//app.example/cb", "", false},
		{"/relative/path", "", false},
		{"http://[::1]:8080/x", "http://[::1]:8080", true},
		{"http://app.example\r\n/evil", "", false},
		{"http://app.exa mple/", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := OriginOf(tc.in)
		if ok != tc.wantOK || (ok && got != tc.want) {
			t.Errorf("OriginOf(%q) = %q,%v want %q,%v", tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}
