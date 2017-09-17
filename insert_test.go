package sqlz

import "testing"

func TestInsert(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			test{
				"simple insert",
				dbz.InsertInto("table").Columns("id", "name", "date").Values(1, "My Name", 96969696),
				"INSERT INTO table (id, name, date) VALUES (?, ?, ?)",
				[]interface{}{1, "My Name", 96969696},
			},

			test{
				"insert with value map",
				dbz.InsertInto("table").ValueMap(map[string]interface{}{"id": 1, "name": "My Name"}),
				"INSERT INTO table (id, name) VALUES (?, ?)",
				[]interface{}{1, "My Name"},
			},

			test{
				"insert with returning clause",
				dbz.InsertInto("table").Columns("one", "two").Values(1, 2).Returning("id"),
				"INSERT INTO table (one, two) VALUES (?, ?) RETURNING id",
				[]interface{}{1, 2},
			},

			test{
				"insert with on conflict do nothing clause",
				dbz.InsertInto("table").Columns("one", "two").Values(1, 2).OnConflictDoNothing(),
				"INSERT INTO table (one, two) VALUES (?, ?) ON CONFLICT DO NOTHING",
				[]interface{}{1, 2},
			},

			test{
				"insert rows from a select query",
				dbz.InsertInto("table").Columns("one", "two").FromSelect(
					dbz.Select("*").From("table2"),
				),
				"INSERT INTO table (one, two) SELECT * FROM table2",
				[]interface{}{},
			},

			test{
				"insert with an on conflict do update",
				dbz.InsertInto("table").Columns("name").Values("My Name").
					OnConflict(
						OnConflict("name", "something_else").
							DoUpdate().
							Set("update_date", 55151515).
							SetMap(map[string]interface{}{
								"name":    "My Name Again",
								"address": "Some Address",
							})),
				"INSERT INTO table (name) VALUES (?) ON CONFLICT (name, something_else) DO UPDATE SET update_date = ?, name = ?, address = ?",
				[]interface{}{"My Name", 55151515, "My Name Again", "Some Address"},
			},
		}
	})
}
