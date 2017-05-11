package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DeleteStmt struct {
	Table      string
	Conditions []WhereCondition
	execer     sqlx.Execer
}

func (db *DB) DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:  table,
		execer: db.DB,
	}
}

func (tx *Tx) DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:  table,
		execer: tx.Tx,
	}
}

func (stmt *DeleteStmt) Where(conds ...WhereCondition) *DeleteStmt {
	stmt.Conditions = append(stmt.Conditions, conds...)
	return stmt
}

func (stmt *DeleteStmt) ToSQL(_ bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"DELETE FROM " + stmt.Table}

	if len(stmt.Conditions) > 0 {
		whereClause, whereBindings := parseConditions(stmt.Conditions)
		bindings = append(bindings, whereBindings...)
		clauses = append(clauses, "WHERE "+whereClause)
	}

	asSQL = strings.Join(clauses, " ")
	if db, ok := stmt.execer.(*sqlx.DB); ok {
		asSQL = db.Rebind(asSQL)
	} else if tx, ok := stmt.execer.(*sqlx.Tx); ok {
		asSQL = tx.Rebind(asSQL)
	}

	return asSQL, bindings
}

func (stmt *DeleteStmt) Exec() (res sql.Result, err error) {
	asSQL, bindings := stmt.ToSQL(true)
	return stmt.execer.Exec(asSQL, bindings...)
}
