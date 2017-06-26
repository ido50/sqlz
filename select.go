package sqlz

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// JoinType is an enumerated type representing the
// type of a JOIN clause (INNER, LEFT, RIGHT or FULL)
type JoinType int

// String returns the string representation of the
// join type (e.g. "FULL JOIN")
func (j JoinType) String() string {
	return []string{"INNER", "LEFT", "RIGHT", "FULL"}[int(j)] + " JOIN"
}

// InnerJoin represents an inner join
// LeftJoin represents a left join
// RightJoin represents a right join
// FullJoin represents a full join
const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	IsDistinct      bool
	Columns         []string
	Table           string
	Joins           []JoinClause
	Conditions      []WhereCondition
	Ordering        []OrderColumn
	Grouping        []string
	GroupConditions []WhereCondition
	LimitTo         int64
	OffsetFrom      int64
	OffsetRows      int64
	queryer         sqlx.Queryer
}

// JoinClause represents a JOIN clause in a
// SELECT statement
type JoinClause struct {
	Type       JoinType
	Table      string
	ResultSet  *SelectStmt
	Conditions []WhereCondition
}

// OrderColumn represents a column in an ORDER BY
// clause (with direction)
type OrderColumn struct {
	Column string
	Desc   bool
}

// ToSQL generates SQL for an OrderColumn
func (o OrderColumn) ToSQL() string {
	str := o.Column
	if o.Desc {
		str += " DESC"
	} else {
		str += " ASC"
	}
	return str
}

// Asc creates an OrderColumn for the provided
// column in ascending order
func Asc(col string) OrderColumn {
	return OrderColumn{col, false}
}

// Desc creates an OrderColumn for the provided
// column in descending order
func Desc(col string) OrderColumn {
	return OrderColumn{col, true}
}

// Select creates a new SelectStmt object, selecting
// the provided columns. You can use any SQL syntax
// supported by your database system, e.g. Select("*"),
// Select("one", "two t", "MAX(three) maxThree")
func (db *DB) Select(cols ...string) *SelectStmt {
	return &SelectStmt{
		Columns: append([]string{}, cols...),
		queryer: db.DB,
	}
}

// Select creates a new SelectStmt object, selecting
// the provided columns. You can use any SQL syntax
// supported by your database system, e.g. Select("*"),
// Select("one", "two t", "MAX(three) maxThree")
func (tx *Tx) Select(cols ...string) *SelectStmt {
	return &SelectStmt{
		Columns: append([]string{}, cols...),
		queryer: tx.Tx,
	}
}

// Distinct marks the statements as a SELECT DISTINCT
// statement
func (stmt *SelectStmt) Distinct() *SelectStmt {
	stmt.IsDistinct = true
	return stmt
}

// From sets the table to select from
func (stmt *SelectStmt) From(table string) *SelectStmt {
	stmt.Table = table
	return stmt
}

// Join creates a new join with the supplied type, on the
// supplied table or result set (a sub-select statement),
// using the provided conditions. Since conditions in a
// JOIN clause usually compare two columns, use sqlz.Indirect
// in your conditions.
func (stmt *SelectStmt) Join(joinType JoinType, table string, resultSet *SelectStmt, conds ...WhereCondition) *SelectStmt {
	stmt.Joins = append(stmt.Joins, JoinClause{
		Type:       joinType,
		Table:      table,
		ResultSet:  resultSet,
		Conditions: append([]WhereCondition{}, conds...),
	})
	return stmt
}

// LeftJoin is a wrapper of Join for creating a LEFT JOIN on a table
// with the provided conditions
func (stmt *SelectStmt) LeftJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(LeftJoin, table, nil, conds...)
}

// RightJoin is a wrapper of Join for creating a RIGHT JOIN on a table
// with the provided conditions
func (stmt *SelectStmt) RightJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(RightJoin, table, nil, conds...)
}

// InnerJoin is a wrapper of Join for creating a INNER JOIN on a table
// with the provided conditions
func (stmt *SelectStmt) InnerJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(InnerJoin, table, nil, conds...)
}

// FullJoin is a wrapper of Join for creating a FULL JOIN on a table
// with the provided conditions
func (stmt *SelectStmt) FullJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(FullJoin, table, nil, conds...)
}

// LeftJoinRS is a wrapper of Join for creating a LEFT JOIN on the
// results of a sub-query
func (stmt *SelectStmt) LeftJoinRS(rs *SelectStmt, as string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(LeftJoin, as, rs, conds...)
}

// RightJoinRS is a wrapper of Join for creating a RIGHT JOIN on the
// results of a sub-query
func (stmt *SelectStmt) RightJoinRS(rs *SelectStmt, as string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(RightJoin, as, rs, conds...)
}

// InnerJoinRS is a wrapper of Join for creating a INNER JOIN on the
// results of a sub-query
func (stmt *SelectStmt) InnerJoinRS(rs *SelectStmt, as string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(InnerJoin, as, rs, conds...)
}

// FullJoinRS is a wrapper of Join for creating a FULL JOIN on the
// results of a sub-query
func (stmt *SelectStmt) FullJoinRS(rs *SelectStmt, as string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(FullJoin, as, rs, conds...)
}

// Where creates one or more WHERE conditions for the SELECT statement.
// If multiple conditions are passed, they are considered AND conditions.
func (stmt *SelectStmt) Where(conditions ...WhereCondition) *SelectStmt {
	stmt.Conditions = append(stmt.Conditions, conditions...)
	return stmt
}

// OrderBy sets an ORDER BY clause for the query. Pass OrderColumn objects
// using the Asc and Desc functions.
func (stmt *SelectStmt) OrderBy(cols ...OrderColumn) *SelectStmt {
	stmt.Ordering = append(stmt.Ordering, cols...)
	return stmt
}

// GroupBy sets a GROUP BY clause with the provided columns.
func (stmt *SelectStmt) GroupBy(cols ...string) *SelectStmt {
	stmt.Grouping = append(stmt.Grouping, cols...)
	return stmt
}

// Having sets HAVING conditions for aggregated values. Usage is the
// same as Where.
func (stmt *SelectStmt) Having(conditions ...WhereCondition) *SelectStmt {
	stmt.GroupConditions = append(stmt.GroupConditions, conditions...)
	return stmt
}

// Limit limits the amount of results returned to the provided value
// (this is a LIMIT clause). In some database systems, Offset with two
// values should be used instead.
func (stmt *SelectStmt) Limit(limit int64) *SelectStmt {
	stmt.LimitTo = limit
	return stmt
}

// Offset skips the provided number of results. In supporting database
// systems, you can provide a limit on the number of the returned
// results as the second parameter
func (stmt *SelectStmt) Offset(start int64, rows ...int64) *SelectStmt {
	stmt.OffsetFrom = start
	if len(rows) > 0 {
		stmt.OffsetRows = rows[0]
	}
	return stmt
}

// ToSQL generates the SELECT statement's SQL and returns a list of
// bindings. It is used internally by GetRow and GetAll, but is
// exported if you wish to use it directly.
func (stmt *SelectStmt) ToSQL(rebind bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"SELECT"}

	if stmt.IsDistinct {
		clauses = append(clauses, "DISTINCT")
	}

	if len(stmt.Columns) == 0 {
		clauses = append(clauses, "*")
	} else {
		clauses = append(clauses, strings.Join(stmt.Columns, ", "))
	}

	clauses = append(clauses, "FROM "+stmt.Table)

	for _, join := range stmt.Joins {
		onClause, joinBindings := parseConditions(join.Conditions)

		if join.ResultSet != nil {
			rsSQL, rsBindings := join.ResultSet.ToSQL(false)
			clauses = append(clauses, join.Type.String()+" ("+rsSQL+") "+join.Table+" ON "+onClause)
			bindings = append(bindings, rsBindings...)
		} else {
			clauses = append(clauses, join.Type.String()+" "+join.Table+" ON "+onClause)
		}

		// add the join condition bindings (this MUST happen after adding the clause
		// itself, because if the join is on a result set then the result set's bindings
		// need to come first
		bindings = append(bindings, joinBindings...)
	}

	if len(stmt.Conditions) > 0 {
		whereClause, whereBindings := parseConditions(stmt.Conditions)
		bindings = append(bindings, whereBindings...)
		clauses = append(clauses, "WHERE "+whereClause)
	}

	if len(stmt.Grouping) > 0 {
		clauses = append(clauses, "GROUP BY "+strings.Join(stmt.Grouping, ", "))
	}

	if len(stmt.GroupConditions) > 0 {
		groupByClause, groupBindings := parseConditions(stmt.GroupConditions)
		bindings = append(bindings, groupBindings...)
		clauses = append(clauses, "HAVING "+groupByClause)
	}

	if len(stmt.Ordering) > 0 {
		var ordering []string
		for _, order := range stmt.Ordering {
			ordering = append(ordering, order.ToSQL())
		}
		clauses = append(clauses, "ORDER BY "+strings.Join(ordering, ", "))
	}

	if stmt.LimitTo > 0 {
		clauses = append(clauses, fmt.Sprintf("LIMIT %d", stmt.LimitTo))
	}

	if stmt.OffsetFrom > 0 {
		offset := fmt.Sprintf("%d", stmt.OffsetFrom)
		if stmt.OffsetRows > 0 {
			offset += fmt.Sprintf(" %d", stmt.OffsetRows)
		}
		clauses = append(clauses, "OFFSET "+offset)
	}

	asSQL = strings.Join(clauses, " ")

	if rebind {
		if db, ok := stmt.queryer.(*sqlx.DB); ok {
			asSQL = db.Rebind(asSQL)
		} else if tx, ok := stmt.queryer.(*sqlx.Tx); ok {
			asSQL = tx.Rebind(asSQL)
		}
	}

	return asSQL, bindings
}

// GetRow executes the SELECT statement and loads the first
// result into the provided variable (which may be a simple
// variable if only one column was selected, or a struct if
// multiple columns were selected).
func (stmt *SelectStmt) GetRow(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Get(stmt.queryer, into, asSQL, bindings...)
}

// GetAll executes the SELECT statement and loads all the
// results into the provided slice variable.
func (stmt *SelectStmt) GetAll(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Select(stmt.queryer, into, asSQL, bindings...)
}

// GetCount executes the SELECT statement disregarding limits,
// offsets, selected columns and ordering; and returns the
// total number of matching results. This is useful when
// paginating results.
func (stmt *SelectStmt) GetCount() (count int64, err error) {
	var countStmt SelectStmt
	countStmt = *stmt
	countStmt.Columns = []string{"COUNT(*)"}
	countStmt.LimitTo = 0
	countStmt.OffsetFrom = 0
	countStmt.OffsetRows = 0
	countStmt.Ordering = []OrderColumn{}

	err = countStmt.GetRow(&count)
	return count, err
}
