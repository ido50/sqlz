package sqlz

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			{
				"simple update",
				dbz.Update("table").Set("something", 3).Set("something-else", true),
				"UPDATE table SET something = ?, something-else = ?",
				[]interface{}{3, true},
			},

			{
				"simple update with map of updates",
				dbz.Update("table").SetMap(map[string]interface{}{"something": 3, "something-else": true}),
				"UPDATE table SET something = ?, something-else = ?",
				[]interface{}{3, true},
			},

			{
				"update with where clause",
				dbz.Update("table").Set("something", 3).Where(Eq("id", 123), Gte("date", 109234234)),
				"UPDATE table SET something = ? WHERE id = ? AND date >= ?",
				[]interface{}{3, 123, 109234234},
			},

			{
				"update with returning clause",
				dbz.Update("table").Set("something", nil).Where(Eq("id", 123)).Returning("something-else"),
				"UPDATE table SET something = ? WHERE id = ? RETURNING something-else",
				[]interface{}{nil, 123},
			},

			{
				"update with update functions",
				dbz.Update("table").Set("something", 3).Set("things", ArrayAppend("things", "asdf")),
				"UPDATE table SET something = ?, things = array_append(things, ?)",
				[]interface{}{3, "asdf"},
			},

			{
				"update with conditional set (true)",
				dbz.Update("table").Set("something", 3).SetIf("other", 2, 3 > 1),
				"UPDATE table SET other = ?, something = ?",
				[]interface{}{2, 3},
			},

			{
				"update with conditional set (false)",
				dbz.Update("table").Set("something", 3).SetIf("other", 2, 3 == 1),
				"UPDATE table SET something = ?",
				[]interface{}{3},
			},

			{
				"update that uses a function with bindings",
				dbz.Update("table").Set("something", Indirect("replace(something, ?, '')", "prefix/")),
				"UPDATE table SET something = replace(something, ?, '')",
				[]interface{}{"prefix/"},
			},
		}
	})
}
