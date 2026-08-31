package benchrun

import (
	"testing"
)

func TestBenchmarkDSNGuards(t *testing.T) {
	for _, dsn := range []string{"postgres://user:password@localhost/instant_bench_x", "postgres://localhost/production", "host=shared-db dbname=x", "postgres://user:password@10.0.0.1/instant_bench_x"} {
		if dsn == "postgres://user:password@localhost/instant_bench_x" {
			if err := ValidateBenchmarkDSN(dsn); err != nil {
				t.Fatalf("local credential-bearing DSN should be accepted: %v", err)
			}
			continue
		}
		if ValidateBenchmarkDSN(dsn) == nil {
			t.Fatalf("unsafe DSN accepted: %q", dsn)
		}
	}
	if err := ValidateBenchmarkDSN("host=127.0.0.1 port=5432 dbname=instant_bench_x"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBenchmarkDSN("postgres://localhost/instant_bench_x?host=remote.example"); err == nil {
		t.Fatal("URI host override bypassed loopback validation")
	}
}
