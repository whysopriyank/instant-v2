package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCorpusReplayUsesSourceLayout(t *testing.T) {
	for _, layout := range []string{"cmd", "tools"} {
		t.Run(layout, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, layout, "corpusctl")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n"), 0600); err != nil {
				t.Fatal(err)
			}
			command := corpusReplayCommand(root, "ws://127.0.0.1:1")
			if command.Dir != root || command.Args[2] != "./"+layout+"/corpusctl" {
				t.Fatalf("source layout %s: dir=%s args=%v", layout, command.Dir, command.Args)
			}
		})
	}
}
