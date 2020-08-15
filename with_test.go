package sqlz

import (
	"testing"
)

func TestWith(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			{
				"WITH with one auxiliary query",
				dbz.With(
					dbz.Select("id").
						From("table").
						Where(Eq("something", 3)),
					"aux",
				).Then(
					dbz.InsertInto("table2").
						Columns("something_id", "other_value").
						Values(Indirect("aux.id"), 4),
				),
				"WITH aux AS (SELECT id FROM table WHERE something = ?) INSERT INTO table2 (something_id, other_value) VALUES (aux.id, ?)",
				[]interface{}{3, 4},
			},

			{
				"WITH with multiple auxiliary queries",
				dbz.With(
					dbz.Select("id").
						From("table").
						Where(Eq("something", 3)),
					"somethings",
				).And(
					dbz.Select("MAX(value) AS max").
						From("other_table").
						Where(Eq("something", 3)),
					"values",
				).Then(
					dbz.DeleteFrom("ref_table").
						Where(
							Eq("something_id", Indirect("somethings.id")),
							Lt("value", Indirect("values.max")),
						),
				),
				"WITH somethings AS (SELECT id FROM table WHERE something = ?), values AS (SELECT MAX(value) AS max FROM other_table WHERE something = ?) DELETE FROM ref_table WHERE something_id = somethings.id AND value < values.max",
				[]interface{}{3, 3},
			},

			{
				"INSERT query that insert from a WITH-ed SELECT",
				dbz.With(
					dbz.Select("id").
						From("table").
						Where(Eq("something", 3)),
					"somethings",
				).Then(
					dbz.InsertInto("ref_table").
						FromSelect(
							dbz.Select("*").From("somethings"),
						),
				),
				"WITH somethings AS (SELECT id FROM table WHERE something = ?) INSERT INTO ref_table SELECT * FROM somethings",
				[]interface{}{3},
			},
		}
	})
}
