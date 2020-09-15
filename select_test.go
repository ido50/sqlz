package sqlz

import "testing"

func TestSelect(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			{
				"simple select all",
				dbz.Select("*").From("table"),
				"SELECT * FROM table",
				[]interface{}{},
			},

			{
				"simple select all with join",
				dbz.Select("*").From("table").LeftJoin("left-table", Eq("left-col", Indirect("our-col"))).RightJoin("right-table", Eq("right-col", Indirect("our-col"))),
				"SELECT * FROM table LEFT JOIN left-table ON left-col = our-col RIGHT JOIN right-table ON right-col = our-col",
				[]interface{}{},
			},

			{
				"select cols with where clause",
				dbz.Select("id", "name").From("table").Where(Eq("integer-col", 2), Eq("string-col", "string"), Gt("real-col", 3.2)),
				"SELECT id, name FROM table WHERE integer-col = ? AND string-col = ? AND real-col > ?",
				[]interface{}{2, "string", 3.2},
			},

			{
				"select distinct with ordering",
				dbz.Select("id").Distinct().From("table").OrderBy(Desc("id")),
				"SELECT DISTINCT id FROM table ORDER BY id DESC",
				[]interface{}{},
			},

			{
				"select cols with ordering, group by and having",
				dbz.Select("one", "two").From("table").Where(Like("name", "prefix%"), IsNotNull("nullable-col")).GroupBy("some-id").Having(Gte("MAX(some-int)", 3)).OrderBy(Asc("one"), Desc("two")),
				"SELECT one, two FROM table WHERE name LIKE ? AND nullable-col IS NOT NULL GROUP BY some-id HAVING MAX(some-int) >= ? ORDER BY one ASC, two DESC",
				[]interface{}{"prefix%", 3},
			},

			{
				"select with a join on another select",
				dbz.Select("a.id, a.value").From("table a").Where(Eq("a.id", 1)).InnerJoinRS(
					dbz.Select("id, MAX(value) value").From("table").GroupBy("id"),
					"b",
					Eq("a.id", Indirect("b.id")),
				),
				"SELECT a.id, a.value FROM table a INNER JOIN (SELECT id, MAX(value) value FROM table GROUP BY id) b ON a.id = b.id WHERE a.id = ?",
				[]interface{}{1},
			},

			{
				"select with array comparisons",
				dbz.Select("*").From("table").Where(EqAny("array_col", 3), GtAll("other_array_col", 1), NeAny("yet_another_col", Indirect("NOW()")),
					Any(Indirect("column"),[]int{1,2,3})),
				"SELECT * FROM table WHERE ? = ANY(array_col) AND ? > ALL(other_array_col) AND NOW() <> ANY(yet_another_col) AND column = ANY(?)",
				[]interface{}{3, 1 ,"'{1,2,3}'"},
			},

			{
				"select with IN condition",
				dbz.Select("*").From("table").Where(In("id", 1, 2, 3, 4)),
				"SELECT * FROM table WHERE id IN (?, ?, ?, ?)",
				[]interface{}{1, 2, 3, 4},
			},

			{
				"select with multiple IN conditions",
				dbz.Select("*").From("table").Where(Or(In("one", 3, 4), NotIn("two", "a", "b"))),
				"SELECT * FROM table WHERE one IN (?, ?) OR two NOT IN (?, ?)",
				[]interface{}{3, 4, "a", "b"},
			},

			{
				"select with both IN and simple conditions",
				dbz.Select("*").From("table").Where(In("one", 3, 4), Eq("id", "a")),
				"SELECT * FROM table WHERE one IN (?, ?) AND id = ?",
				[]interface{}{3, 4, "a"},
			},

			{
				"select with both simple and SQL conditions",
				dbz.Select("*").From("table").Where(Eq("one", 2), SQLCond("? LIKE some_col", "bla")),
				"SELECT * FROM table WHERE one = ? AND ? LIKE some_col",
				[]interface{}{2, "bla"},
			},

			{
				"select for update",
				dbz.Select("*").From("table").Where(Eq("id", 1)).Lock(ForUpdate()),
				"SELECT * FROM table WHERE id = ? FOR UPDATE",
				[]interface{}{1},
			},

			{
				"select for no key update of table without waiting",
				dbz.Select("*").From("table").Lock(ForNoKeyUpdate().OfTables("table").NoWait()),
				"SELECT * FROM table FOR NO KEY UPDATE OF table NOWAIT",
				[]interface{}{},
			},

			{
				"select with a left lateral join",
				dbz.Select("a.id, a.value").From("table a").Where(Eq("a.id", 1)).LeftLateralJoin(
					dbz.Select("id, MAX(value) value").From("table").GroupBy("id"),
					"b",
					Eq("a.id", Indirect("b.id")),
				),
				"SELECT a.id, a.value FROM table a LEFT JOIN LATERAL (SELECT id, MAX(value) value FROM table GROUP BY id) b ON a.id = b.id WHERE a.id = ?",
				[]interface{}{1},
			},
			{
				"select with an inner lateral join",
				dbz.Select("a.id, a.value").From("table a").Where(Eq("a.id", 1)).InnerLateralJoin(
					dbz.Select("id, MAX(value) value").From("table").GroupBy("id"),
					"b",
					SQLCond("True"),
				),
				"SELECT a.id, a.value FROM table a INNER JOIN LATERAL (SELECT id, MAX(value) value FROM table GROUP BY id) b ON True WHERE a.id = ?",
				[]interface{}{1},
			},
			{
				"select with a right lateral join",
				dbz.Select("a.id, a.value").From("table a").Where(Eq("a.id", 1)).RightLateralJoin(
					dbz.Select("count").From("table").Where(Gt("a.value", 0)),
					"counts",
					Eq("a.id", Indirect("b.id")),
				),
				"SELECT a.id, a.value FROM table a RIGHT JOIN LATERAL (SELECT count FROM table WHERE a.value > ?) counts ON a.id = b.id WHERE a.id = ?",
				[]interface{}{0, 1},
			},
			{
				"select with a single union",
				dbz.Select("a.name").From("table a").Where(Eq("a.name", "a")).Union(
					dbz.Select("b.name").From("table b").Where(Eq("b.name", "b"))),
				"SELECT a.name FROM table a WHERE a.name = ? UNION SELECT b.name FROM table b WHERE b.name = ?",
				[]interface{}{"a", "b"},
			},
			{
				"select with a single union all",
				dbz.Select("a.name").From("table a").Where(Eq("a.name", "a")).UnionAll(
					dbz.Select("b.name").From("table b").Where(Eq("b.name", "b"))),
				"SELECT a.name FROM table a WHERE a.name = ? UNION ALL SELECT b.name FROM table b WHERE b.name = ?",
				[]interface{}{"a", "b"},
			},
			{
				"select with multiple unions",
				dbz.Select("a.name").From("table a").Where(Eq("a.name", "a")).Union(
					dbz.Select("b.name").From("table b").Where(Eq("b.name", "b"))).Union(
					dbz.Select("c.name").From("table c").Where(Eq("c.name", "c"))),
				"SELECT a.name FROM table a WHERE a.name = ? " +
					"UNION SELECT b.name FROM table b WHERE b.name = ? " +
					"UNION SELECT c.name FROM table c WHERE c.name = ?",
				[]interface{}{"a", "b", "c"},
			},
		}
	})
}
