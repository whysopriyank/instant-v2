package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestDaemonZIPRestoreFilesAreDownloadable(t *testing.T) {
	mux, _, token := cf003PostgresMux(t)
	var archive bytes.Buffer
	w := zip.NewWriter(&archive)
	const location = "00000000-0000-4000-8000-000000000006"
	const contents = "restored through the assembled daemon"
	entries := []struct{ name, contents string }{
		{"config.json", `{"title":"restored files","schema":{"entities":{"$files":{"attrs":{"path":{"valueType":"string","config":{"unique":true,"indexed":true}}}}}}}`},
		{"entities/$files.jsonl", fmt.Sprintf(`{"entity":{"id":%q,"path":"restored.txt","location-id":%q,"size":%d,"content-type":"text/plain","content-disposition":"inline","key-version":1},"createdAt":1700000000000}`+"\n", cf003FileID, location, len(contents))},
		{"files/" + location, contents},
	}
	for _, entry := range entries {
		body, err := w.Create(entry.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := body.Write([]byte(entry.contents)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/backup/"+cf003AppID+"/restore-v1zip", bytes.NewReader(archive.Bytes()))
	request.Header.Set("X-admin-token", token)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ZIP restore status=%d body=%s", response.Code, response.Body.String())
	}
	status, body, _ := cf003Serve(mux, http.MethodGet, "/storage/signed-download-url?app-id="+cf003AppID+"&id="+cf003FileID, "", map[string]string{"X-admin-token": token})
	if status != http.StatusOK {
		t.Fatalf("download URL status=%d body=%s", status, body)
	}
	var signed struct {
		Data struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &signed); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed.Data.URL)
	if err != nil || u.Path == "" {
		t.Fatalf("invalid signed URL %q: %v", signed.Data.URL, err)
	}
	status, body, _ = cf003Serve(mux, http.MethodGet, u.RequestURI(), "", nil)
	if status != http.StatusOK || body != contents {
		t.Fatalf("restored file download status=%d body=%q", status, body)
	}
}
