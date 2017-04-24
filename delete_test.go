package sqlz

import "testing"

func TestDelete(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			test{
				"delete all table",
				dbz.DeleteFrom("table"),
				"DELETE FROM table",
				[]interface{}{},
			},

			test{
				"delete all rows matching condition",
				dbz.DeleteFrom("table").Where(Or(Eq("id", 1), And(Eq("name", "some-name"), Gt("integer", 3)))),
				"DELETE FROM table WHERE id = ? OR (name = ? AND integer > ?)",
				[]interface{}{1, "some-name", 3},
			},
		}
	})
}
