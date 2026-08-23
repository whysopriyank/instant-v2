module github.com/YOU/instant-v2

go 1.24

// Pin versions below to whatever `go get` resolves during Phase 0.
// Leaving them absent until the first `schemagen`+`corpusctl` commits is intentional:
// the module exists as a scaffold, not a promise.

require (
	// jackc/pgx/v5          // postgres driver + pgtype + pgconn
	// jackc/pglogrepl       // pgoutput logical replication (pre-v1; pin commit)
	// github.com/google/cel-go // reference CEL impl
	// github.com/coder/websocket
	// github.com/aws/aws-sdk-go-v2/service/s3
	// github.com/golang-jwt/jwt/v5
	// github.com/MicahParks/jwkset
	// github.com/go-chi/chi/v5 OR stdlib net/http
)
