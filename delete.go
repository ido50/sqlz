package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

// DeleteStmt represents a DELETE statement
type DeleteStmt struct {
	Table      string
	Conditions []WhereCondition
	execer     sqlx.Execer
}

// DeleteFrom creates a new DeleteStmt object for the
// provided table
func (db *DB) DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:  table,
		execer: db.DB,
	}
}

// DeleteFrom creates a new DeleteStmt object for the
// provided table
func (tx *Tx) DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:  table,
		execer: tx.Tx,
	}
}

// Where creates one or more WHERE conditions for the DELETE statement.
// If multiple conditions are passed, they are considered AND conditions.
func (stmt *DeleteStmt) Where(conds ...WhereCondition) *DeleteStmt {
	stmt.Conditions = append(stmt.Conditions, conds...)
	return stmt
}

// ToSQL generates the DELETE statement's SQL and returns a list of
// bindings. It is used internally by Exec, but is exported if you
// wish to use it directly.
func (stmt *DeleteStmt) ToSQL(rebind bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"DELETE FROM " + stmt.Table}

	if len(stmt.Conditions) > 0 {
		whereClause, whereBindings := parseConditions(stmt.Conditions)
		bindings = append(bindings, whereBindings...)
		clauses = append(clauses, "WHERE "+whereClause)
	}

	asSQL = strings.Join(clauses, " ")

	if rebind {
		if db, ok := stmt.execer.(*sqlx.DB); ok {
			asSQL = db.Rebind(asSQL)
		} else if tx, ok := stmt.execer.(*sqlx.Tx); ok {
			asSQL = tx.Rebind(asSQL)
		}
	}

	return asSQL, bindings
}

// Exec executes the DELETE statement, returning the standard
// sql.Result struct and an error if the query failed.
func (stmt *DeleteStmt) Exec() (res sql.Result, err error) {
	asSQL, bindings := stmt.ToSQL(true)
	return stmt.execer.Exec(asSQL, bindings...)
}
