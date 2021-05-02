package sqlz

import "testing"

func TestSet(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			{
				name:        "simple set",
				stmt:        dbz.Set("key", "value"),
				expectedSQL: "SET key TO value",
			},
			{
				name:        "local set",
				stmt:        dbz.Set("key", "value").Local(),
				expectedSQL: "SET LOCAL key TO value",
			},
			{
				name:        "session set",
				stmt:        dbz.Set("key", "value").Session(),
				expectedSQL: "SET SESSION key TO value",
			},
		}
	})
}
