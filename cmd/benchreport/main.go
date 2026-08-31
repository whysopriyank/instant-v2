package main

import (
	"flag"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"os"
)

func main() {
	root := flag.String("input", "", "bundle root (required)")
	out := flag.String("output", "", "markdown output (default stdout)")
	flag.Parse()
	if *root == "" {
		fmt.Fprintln(os.Stderr, "-input is required")
		os.Exit(2)
	}
	s, e := benchrun.ReportFromArtifacts(*root)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	b := []byte(benchrun.RenderMarkdown(s))
	if *out == "" {
		_, _ = os.Stdout.Write(b)
		return
	}
	if e = os.WriteFile(*out, b, 0640); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
