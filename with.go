package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

// AuxStmt represents an auxiliary statement that is part
// of a WITH query. It includes the statement itself, and
// the name used for referencing it in other queries
type AuxStmt struct {
	Stmt SQLStmt
	As   string
}

// WithStmt represents a WITH statement
type WithStmt struct {
	// AuxStmts is the list of auxiliary statements that are
	// part of the WITH query
	AuxStmts []AuxStmt
	// MainStmt is the query's main statement in which the
	// auxiliary statements can be referenced
	MainStmt SQLStmt

	execer sqlx.Ext
}

// With creates a new WithStmt object including
// the provided auxiliary statements
func (db *DB) With(stmt SQLStmt, as string) *WithStmt {
	return &WithStmt{
		AuxStmts: []AuxStmt{{stmt, as}},
		execer:   db.DB,
	}
}

// With creates a new WithStmt object including
// the provided auxiliary statements
func (tx *Tx) With(stmt SQLStmt, as string) *WithStmt {
	return &WithStmt{
		AuxStmts: []AuxStmt{{stmt, as}},
		execer:   tx.Tx,
	}
}

// And adds another auxiliary statement to the query
func (stmt *WithStmt) And(auxStmt SQLStmt, as string) *WithStmt {
	stmt.AuxStmts = append(stmt.AuxStmts, AuxStmt{auxStmt, as})
	return stmt
}

// Then sets the main statement of the WITH query
func (stmt *WithStmt) Then(mainStmt SQLStmt) *WithStmt {
	stmt.MainStmt = mainStmt
	return stmt
}

// ToSQL generates the WITH statement's SQL and returns a list of
// bindings. It is used internally by Exec, GetRow and GetAll, but is
// exported if you wish to use it directly.
func (stmt *WithStmt) ToSQL(rebind bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"WITH"}

	var auxStmts []string
	for _, aux := range stmt.AuxStmts {
		auxSQL, auxBindings := aux.Stmt.ToSQL(false)
		bindings = append(bindings, auxBindings...)
		auxStmts = append(auxStmts, aux.As+" AS ("+auxSQL+")")
	}

	clauses = append(clauses, strings.Join(auxStmts, ", "))

	mainSQL, mainBindings := stmt.MainStmt.ToSQL(false)
	clauses = append(clauses, mainSQL)
	bindings = append(bindings, mainBindings...)

	asSQL = strings.Join(clauses, " ")
	if db, ok := stmt.execer.(*sqlx.DB); ok {
		asSQL = db.Rebind(asSQL)
	} else if tx, ok := stmt.execer.(*sqlx.Tx); ok {
		asSQL = tx.Rebind(asSQL)
	}

	return asSQL, bindings
}

// Exec executes the WITH statement, returning the standard
// sql.Result struct and an error if the query failed.
func (stmt *WithStmt) Exec() (res sql.Result, err error) {
	asSQL, bindings := stmt.ToSQL(true)
	return stmt.execer.Exec(asSQL, bindings...)
}

// GetRow executes a WITH statement whose main statement has
// a RETURNING clause expected to return one row, and loads
// the result into the provided variable (which may be a
// simple variable if only one column is returned, or a
// struct if multiple columns are returned)
func (stmt *WithStmt) GetRow(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Get(stmt.execer, into, asSQL, bindings...)
}

// GetAll executes a WITH statement whose main statement has
// a RETURNING clause expected to return multiple rows, and
// loads the result into the provided slice variable
func (stmt *WithStmt) GetAll(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Select(stmt.execer, into, asSQL, bindings...)
}
