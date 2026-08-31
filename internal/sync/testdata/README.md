# Node-list wire fixtures

`nodelist-wire.json` contains exact output strings captured before the approved
node-list simplification on 2026-08-31. The baseline was commit
`5f78ca0877c1a8c04b173e5c502e8c948ecfb965` plus the existing user-owned typed encoder
and differential tests. All typed/generic comparisons passed before capture.

The 82 entries retain the four parity examples, one deep-value example, all 72
seeded differential cases (seed `0xA2C105ED`), and five escaping/depth cases. Test
names changed to describe wire behavior rather than the removed implementation.
Input generation and numeric spelling/JSON-validity assertions remain in
`nodelist_test.go`; number-fidelity and ID/ref assertions remain separate tests.

These fixtures are compatibility evidence, not outputs to regenerate merely
because the current encoder changes. Update them only after reviewing an
explicit wire-contract change. No duplicate projection algorithm is retained as
a test oracle.

## Encoder tradeoff

The approved default is one generic `UseNumber` projection and `json.Marshal`.
The removed specialized path duplicated the complete projection and recursively
serialized JSON, falling back to the generic algorithm for escaping/deep values.
An ephemeral side-by-side benchmark confirmed exact output on the unchanged
300-entity `BenchmarkBuildNodeList` fixture before the comparison code was removed.

On Go 1.27.0, darwin/arm64 (Apple M4 Pro), three 100ms samples measured:

| Encoder | Bytes/op | Allocations/op |
|---|---:|---:|
| Previous typed default | 932,289–932,480 | 9,027–9,028 |
| Previous generic fallback | 1,045,267–1,118,068 | 15,216–15,221 |
| Canonical generic default | 1,067,520–1,073,059 | 15,218–15,219 |

The extra allocations versus the typed default are an explicit readability and
single-implementation tradeoff, not a performance improvement. Private named
wire containers were also evaluated and discarded because they added allocation
overhead beyond the original generic tree. Host contention made timing samples
unsuitable for a throughput conclusion; no end-to-end performance claim follows
from this component benchmark. The retained benchmark measures only the final
canonical encoder and excludes fixture setup.
