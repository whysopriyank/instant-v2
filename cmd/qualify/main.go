// Command qualify is the QH-001 Linux qualification harness CLI. It produces
// profile-selected external records and the campaign manifest consumed by
// scripts/quality-release-gate.sh. Public adapters validate retained runtime
// observations; they do not provision environments. It makes no product-code change and never prints
// DSNs or secrets.
package main

import (
	"fmt"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "verify-public":
		return runVerifyPublic(args[1:])
	case "container":
		return runPublicFacts("OP-004", args[1:])
	case "v1-differential":
		return runPublicFacts("CF-005", args[1:])
	case "restore":
		return runPublicFacts("OP-006", args[1:])
	case "native":
		return runNative(args[1:])
	case "recovery":
		return runRecovery(args[1:])
	case "soak":
		return runSoak(args[1:])
	case "record":
		return runRecord(args[1:])
	case "manifest":
		return runManifest(args[1:])
	case "-h", "--help", "help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "qualify: unknown subcommand %q\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: qualify <native|recovery|soak|container|restore|v1-differential|record|manifest|verify-public> [flags]

  native    run/parse the owned-DB + hermetic go test selection (OP-003 lane result)
  recovery  drive the candidate binary through 7 crash/drain outcomes (OP-005 lane result)
  soak      seed via soaksetup and run cmd/soak at alpha budgets (QR-001 lane result)
  container validate retained live container observations (OP-004)
  restore   validate retained live restore/capacity/rejection observations (OP-006)
  v1-differential validate actual corpusctl raw differential frames (CF-005)
  verify-public verify public manifest against candidate-owned policy
  record    assemble one gate-schema external record from a lane result
  manifest  assemble the gate manifest + per-packet handoff files`)
}
