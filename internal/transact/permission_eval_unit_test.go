package transact

import (
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/perms"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSnapshotBindingsDeepCopiesMutableValues(t *testing.T) {
	timeValue := timestamppb.New(time.Unix(123, 456))
	original := perms.Bindings{
		Data:       map[string]any{"typed": []string{"before"}, "bytes": []byte{1}, "nested": map[string]int{"count": 1}},
		NewData:    map[string]any{"items": []map[string]any{{"value": "before"}}},
		Auth:       map[string]any{"roles": []string{"reader"}},
		RuleParams: map[string]any{"limits": []int{1, 2}},
		LinkedData: map[string]any{"ref": map[string]string{"value": "before"}},
		Actions:    map[string]any{"names": []string{"before"}},
		Request: perms.RequestInfo{
			ModifiedFields: []string{"before"},
			Time:           timeValue,
		},
	}
	snapshot := snapshotBindings(original)

	original.Data["typed"].([]string)[0] = "after"
	original.Data["bytes"].([]byte)[0] = 2
	original.Data["nested"].(map[string]int)["count"] = 2
	original.NewData["items"].([]map[string]any)[0]["value"] = "after"
	original.Auth["roles"].([]string)[0] = "writer"
	original.RuleParams["limits"].([]int)[0] = 9
	original.LinkedData["ref"].(map[string]string)["value"] = "after"
	original.Actions["names"].([]string)[0] = "after"
	original.Request.ModifiedFields[0] = "after"
	timeValue.Seconds = 999

	if got := snapshot.Data["typed"].([]string)[0]; got != "before" {
		t.Fatalf("typed data alias: %q", got)
	}
	if got := snapshot.Data["bytes"].([]byte)[0]; got != 1 {
		t.Fatalf("byte data alias: %d", got)
	}
	if got := snapshot.Data["nested"].(map[string]int)["count"]; got != 1 {
		t.Fatalf("nested data alias: %d", got)
	}
	if got := snapshot.NewData["items"].([]map[string]any)[0]["value"]; got != "before" {
		t.Fatalf("newData alias: %v", got)
	}
	if got := snapshot.Auth["roles"].([]string)[0]; got != "reader" {
		t.Fatalf("auth alias: %q", got)
	}
	if got := snapshot.RuleParams["limits"].([]int)[0]; got != 1 {
		t.Fatalf("ruleParams alias: %d", got)
	}
	if got := snapshot.LinkedData["ref"].(map[string]string)["value"]; got != "before" {
		t.Fatalf("linkedData alias: %q", got)
	}
	if got := snapshot.Actions["names"].([]string)[0]; got != "before" {
		t.Fatalf("actions alias: %q", got)
	}
	if got := snapshot.Request.ModifiedFields[0]; got != "before" {
		t.Fatalf("modified fields alias: %q", got)
	}
	if got := snapshot.Request.Time.Seconds; got != 123 {
		t.Fatalf("request time alias: %d", got)
	}
}
