package reactive

import "testing"

func TestTopicRoutingSnapshotOwnsSlice(t *testing.T) {
	store := NewStore()
	for _, sub := range []*Subscription{
		{ID: "b", AppID: "app", Topics: map[string]bool{"shared": true}},
		{ID: "a", AppID: "app", Topics: map[string]bool{"shared": true, "other": true}},
	} {
		if _, err := store.Add(sub); err != nil {
			t.Fatal(err)
		}
	}
	first := store.SubsForTopics([]string{"shared", "other", "shared"})
	second := store.SubsForTopics([]string{"other"})
	if len(first) != 2 || first[0].ID != "a" || first[1].ID != "b" {
		t.Fatalf("sorted, unique first snapshot lost after scratch reuse: %v", first)
	}
	if len(second) != 1 || second[0] != first[0] {
		t.Fatalf("second snapshot must retain live subscription identity: %v", second)
	}
	second[0] = nil
	if first[0] == nil {
		t.Fatal("routing snapshots share their output slice")
	}
}
