// Command qualify is the QH-001 Linux qualification harness CLI. It produces
// the three external records scripts/quality-release-gate.sh requires
// (native_linux → OP-003, recovery → OP-005, soak → QR-001) plus the campaign
// manifest it consumes. It makes no product-code change and never prints
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
	fmt.Fprintln(os.Stderr, `usage: qualify <native|recovery|soak|record|manifest> [flags]

  native    run/parse the owned-DB + hermetic go test selection (OP-003 lane result)
  recovery  drive the candidate binary through 7 crash/drain outcomes (OP-005 lane result)
  soak      seed via soaksetup and run cmd/soak at alpha budgets (QR-001 lane result)
  record    assemble one gate-schema external record from a lane result
  manifest  assemble the gate manifest + per-packet handoff files`)
}
