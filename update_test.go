package sqlz

import "testing"

func TestUpdate(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			test{
				"simple update",
				dbz.Update("table").Set("something", 3).Set("something-else", true),
				"UPDATE table SET something = ?, something-else = ?",
				[]interface{}{3, true},
			},

			test{
				"simple update with map of updates",
				dbz.Update("table").SetMap(map[string]interface{}{"something": 3, "something-else": true}),
				"UPDATE table SET something = ?, something-else = ?",
				[]interface{}{3, true},
			},

			test{
				"update with where clause",
				dbz.Update("table").Set("something", 3).Where(Eq("id", 123), Gte("date", 109234234)),
				"UPDATE table SET something = ? WHERE id = ? AND date >= ?",
				[]interface{}{3, 123, 109234234},
			},

			test{
				"update with returning clause",
				dbz.Update("table").Set("something", nil).Where(Eq("id", 123)).Returning("something-else"),
				"UPDATE table SET something = ? WHERE id = ? RETURNING something-else",
				[]interface{}{nil, 123},
			},

			test{
				"update with update functions",
				dbz.Update("table").Set("something", 3).Set("things", ArrayAppend("things", "asdf")),
				"UPDATE table SET something = ?, things = array_append(things, ?)",
				[]interface{}{3, "asdf"},
			},

			test{
				"update with conditional set (true)",
				dbz.Update("table").Set("something", 3).SetIf("other", 2, 3 > 1),
				"UPDATE table SET something = ?, other = ?",
				[]interface{}{3, 2},
			},

			test{
				"update with conditional set (false)",
				dbz.Update("table").Set("something", 3).SetIf("other", 2, 3 == 1),
				"UPDATE table SET something = ?",
				[]interface{}{3},
			},
		}
	})
}
