package sqlz

import "testing"

func TestSelect(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			test{
				"simple select all",
				dbz.Select("*").From("table"),
				"SELECT * FROM table",
				[]interface{}{},
			},

			test{
				"simple select all with join",
				dbz.Select("*").From("table").LeftJoin("left-table", Eq("left-col", Indirect("our-col"))).RightJoin("right-table", Eq("right-col", Indirect("our-col"))),
				"SELECT * FROM table LEFT JOIN left-table ON left-col = our-col RIGHT JOIN right-table ON right-col = our-col",
				[]interface{}{},
			},

			test{
				"select cols with where clause",
				dbz.Select("id", "name").From("table").Where(Eq("integer-col", 2), Eq("string-col", "string"), Gt("real-col", 3.2)),
				"SELECT id, name FROM table WHERE integer-col = ? AND string-col = ? AND real-col > ?",
				[]interface{}{2, "string", 3.2},
			},

			test{
				"select distinct with ordering",
				dbz.Select("id").Distinct().From("table").OrderBy(Desc("id")),
				"SELECT DISTINCT id FROM table ORDER BY id DESC",
				[]interface{}{},
			},

			test{
				"select cols with ordering, group by and having",
				dbz.Select("one", "two").From("table").Where(Like("name", "prefix%"), IsNotNull("nullable-col")).GroupBy("some-id").Having(Gte("MAX(some-int)", 3)).OrderBy(Asc("one"), Desc("two")),
				"SELECT one, two FROM table WHERE name LIKE ? AND nullable-col IS NOT NULL GROUP BY some-id HAVING MAX(some-int) >= ? ORDER BY one ASC, two DESC",
				[]interface{}{"prefix%", 3},
			},

			test{
				"select with a join on another select",
				dbz.Select("a.id, a.value").From("table a").Where(Eq("a.id", 1)).InnerJoinRS(
					dbz.Select("id, MAX(value) value").From("table").GroupBy("id"),
					"b",
					Eq("a.id", Indirect("b.id")),
				),
				"SELECT a.id, a.value FROM table a INNER JOIN (SELECT id, MAX(value) value FROM table GROUP BY id) b ON a.id = b.id WHERE a.id = ?",
				[]interface{}{1},
			},

			test{
				"select with array comparisons",
				dbz.Select("*").From("table").Where(EqAny("array_col", 3), GtAll("other_array_col", 1), NeAny("yet_another_col", Indirect("NOW()"))),
				"SELECT * FROM table WHERE ? = ANY(array_col) AND ? > ALL(other_array_col) AND NOW() <> ANY(yet_another_col)",
				[]interface{}{3, 1},
			},
		}
	})
}
