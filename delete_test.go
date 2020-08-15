package sqlz

import (
	"testing"
)

func TestDelete(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			{
				"delete all table",
				dbz.DeleteFrom("table"),
				"DELETE FROM table",
				[]interface{}{},
			},

			{
				"delete all rows matching condition",
				dbz.DeleteFrom("table").Where(Or(Eq("id", 1), And(Eq("name", "some-name"), Gt("integer", 3)))),
				"DELETE FROM table WHERE id = ? OR (name = ? AND integer > ?)",
				[]interface{}{1, "some-name", 3},
			},

			{
				"delete with returning clause",
				dbz.DeleteFrom("table").Where(Eq("id", 2)).Returning("name"),
				"DELETE FROM table WHERE id = ? RETURNING name",
				[]interface{}{2},
			},

			{
				"delete using join",
				dbz.DeleteFrom("table").Using("other", "another").Where(Eq("other.fk_id", Indirect("table.id")), Eq("another.fk_id", Indirect("table.id"))),
				"DELETE FROM table USING other, another WHERE other.fk_id = table.id AND another.fk_id = table.id",
				[]interface{}{},
			},
		}
	})
}
