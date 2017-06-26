package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	InsCols []string
	InsVals []interface{}
	Table   string
	Return  []string
	execer  sqlx.Ext
}

// InsertInto creates a new InsertStmt object for the
// provided table
func (db *DB) InsertInto(table string) *InsertStmt {
	return &InsertStmt{
		Table:  table,
		execer: db.DB,
	}
}

// InsertInto creates a new InsertStmt object for the
// provided table
func (tx *Tx) InsertInto(table string) *InsertStmt {
	return &InsertStmt{
		Table:  table,
		execer: tx.Tx,
	}
}

// Columns defines the columns to insert. It can be safely
// used alongside ValueMap in the same query, provided Values
// is used immediately after Columns
func (stmt *InsertStmt) Columns(cols ...string) *InsertStmt {
	stmt.InsCols = append(stmt.InsCols, cols...)
	return stmt
}

// Values sets the values to insert to the table (based on the
// columns provided via Columns)
func (stmt *InsertStmt) Values(vals ...interface{}) *InsertStmt {
	stmt.InsVals = append(stmt.InsVals, vals...)
	return stmt
}

// ValueMap receives a map of columns and values to insert
func (stmt *InsertStmt) ValueMap(vals map[string]interface{}) *InsertStmt {
	for col, val := range vals {
		stmt.InsCols = append(stmt.InsCols, col)
		stmt.InsVals = append(stmt.InsVals, val)
	}
	return stmt
}

// Returning sets a RETURNING clause to receive values back from the
// database once executing the INSERT statement. Note that GetRow or
// GetAll must be used to execute the query rather than Exec to get
// back the values.
func (stmt *InsertStmt) Returning(cols ...string) *InsertStmt {
	stmt.Return = append(stmt.Return, cols...)
	return stmt
}

// ToSQL generates the INSERT statement's SQL and returns a list of
// bindings. It is used internally by Exec, GetRow and GetAll, but is
// exported if you wish to use it directly.
func (stmt *InsertStmt) ToSQL(_ bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"INSERT INTO " + stmt.Table}

	if len(stmt.InsCols) > 0 {
		clauses = append(clauses, "("+strings.Join(stmt.InsCols, ", ")+")")
	}

	if len(stmt.InsVals) > 0 {
		var placeholders []string
		for range stmt.InsVals {
			placeholders = append(placeholders, "?")
		}

		clauses = append(clauses, "VALUES ("+strings.Join(placeholders, ", ")+")")
	}

	if len(stmt.Return) > 0 {
		clauses = append(clauses, "RETURNING "+strings.Join(stmt.Return, ", "))
	}

	asSQL = strings.Join(clauses, " ")
	if db, ok := stmt.execer.(*sqlx.DB); ok {
		asSQL = db.Rebind(asSQL)
	} else if tx, ok := stmt.execer.(*sqlx.Tx); ok {
		asSQL = tx.Rebind(asSQL)
	}

	return asSQL, stmt.InsVals
}

// Exec executes the INSERT statement, returning the standard
// sql.Result struct and an error if the query failed.
func (stmt *InsertStmt) Exec() (res sql.Result, err error) {
	asSQL, bindings := stmt.ToSQL(true)
	return stmt.execer.Exec(asSQL, bindings...)
}

// GetRow executes an INSERT statement with a RETURNING clause
// expected to return one row, and loads the result into
// the provided variable (which may be a simple variable if
// only one column is returned, or a struct if multiple columns
// are returned)
func (stmt *InsertStmt) GetRow(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Get(stmt.execer, into, asSQL, bindings...)
}

// GetAll executes an INSERT statement with a RETURNING clause
// expected to return multiple rows, and loads the result into
// the provided slice variable
func (stmt *InsertStmt) GetAll(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Select(stmt.execer, into, asSQL, bindings...)
}
