package sqlz

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type JoinType int

func (j JoinType) String() string {
	return []string{"INNER", "LEFT", "RIGHT", "FULL"}[int(j)] + " JOIN"
}

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

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

type JoinClause struct {
	Type       JoinType
	Table      string
	Conditions []WhereCondition
}

type OrderColumn struct {
	Column string
	Desc   bool
}

func (o OrderColumn) ToSQL() string {
	str := o.Column
	if o.Desc {
		str += " DESC"
	} else {
		str += " ASC"
	}
	return str
}

func Asc(col string) OrderColumn {
	return OrderColumn{col, false}
}

func Desc(col string) OrderColumn {
	return OrderColumn{col, true}
}

func (db *DB) Select(cols ...string) *SelectStmt {
	return &SelectStmt{
		Columns: append([]string{}, cols...),
		queryer: db.DB,
	}
}

func (tx *Tx) Select(cols ...string) *SelectStmt {
	return &SelectStmt{
		Columns: append([]string{}, cols...),
		queryer: tx.Tx,
	}
}

func (stmt *SelectStmt) Distinct() *SelectStmt {
	stmt.IsDistinct = true
	return stmt
}

func (stmt *SelectStmt) From(table string) *SelectStmt {
	stmt.Table = table
	return stmt
}

func (stmt *SelectStmt) Join(joinType JoinType, table string, conds ...WhereCondition) *SelectStmt {
	stmt.Joins = append(stmt.Joins, JoinClause{
		Type:       joinType,
		Table:      table,
		Conditions: append([]WhereCondition{}, conds...),
	})
	return stmt
}

func (stmt *SelectStmt) LeftJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(LeftJoin, table, conds...)
}

func (stmt *SelectStmt) RightJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(RightJoin, table, conds...)
}

func (stmt *SelectStmt) InnerJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(InnerJoin, table, conds...)
}

func (stmt *SelectStmt) FullJoin(table string, conds ...WhereCondition) *SelectStmt {
	return stmt.Join(FullJoin, table, conds...)
}

func (stmt *SelectStmt) Where(conditions ...WhereCondition) *SelectStmt {
	stmt.Conditions = append(stmt.Conditions, conditions...)
	return stmt
}

func (stmt *SelectStmt) OrderBy(cols ...OrderColumn) *SelectStmt {
	stmt.Ordering = append(stmt.Ordering, cols...)
	return stmt
}

func (stmt *SelectStmt) GroupBy(cols ...string) *SelectStmt {
	stmt.Grouping = append(stmt.Grouping, cols...)
	return stmt
}

func (stmt *SelectStmt) Having(conditions ...WhereCondition) *SelectStmt {
	stmt.GroupConditions = append(stmt.GroupConditions, conditions...)
	return stmt
}

func (stmt *SelectStmt) Limit(limit int64) *SelectStmt {
	stmt.LimitTo = limit
	return stmt
}

func (stmt *SelectStmt) Offset(start int64, rows ...int64) *SelectStmt {
	stmt.OffsetFrom = start
	if len(rows) > 0 {
		stmt.OffsetRows = rows[0]
	}
	return stmt
}

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
		bindings = append(bindings, joinBindings...)

		clauses = append(clauses, join.Type.String()+" "+join.Table+" ON "+onClause)
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

func (stmt *SelectStmt) GetRow(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Get(stmt.queryer, into, asSQL, bindings...)
}

func (stmt *SelectStmt) GetAll(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Select(stmt.queryer, into, asSQL, bindings...)
}

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
