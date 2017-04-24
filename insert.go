package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

type InsertStmt struct {
	InsCols  []string
	Table    string
	Return   []string
	bindings []interface{}
	execer   sqlx.Ext
}

func (db *DB) InsertInto(table string) *InsertStmt {
	return &InsertStmt{
		Table:  table,
		execer: db.DB,
	}
}

func (tx *Tx) InsertInto(table string) *InsertStmt {
	return &InsertStmt{
		Table:  table,
		execer: tx.Tx,
	}
}

func (stmt *InsertStmt) Bindings() []interface{} {
	return stmt.bindings
}

func (stmt *InsertStmt) Columns(cols ...string) *InsertStmt {
	stmt.InsCols = append(stmt.InsCols, cols...)
	return stmt
}

func (stmt *InsertStmt) Values(vals ...interface{}) *InsertStmt {
	stmt.bindings = append(stmt.bindings, vals...)
	return stmt
}

func (stmt *InsertStmt) ValueMap(vals map[string]interface{}) *InsertStmt {
	for col, val := range vals {
		stmt.InsCols = append(stmt.InsCols, col)
		stmt.bindings = append(stmt.bindings, val)
	}
	return stmt
}

func (stmt *InsertStmt) Returning(cols ...string) *InsertStmt {
	stmt.Return = append(stmt.Return, cols...)
	return stmt
}

func (stmt *InsertStmt) ToSQL() (asSQL string, err error) {
	if stmt.Table == "" {
		return asSQL, ErrNoTable
	}

	var clauses = []string{"INSERT INTO " + stmt.Table}

	if len(stmt.InsCols) > 0 {
		clauses = append(clauses, "("+strings.Join(stmt.InsCols, ", ")+")")
	}

	if len(stmt.bindings) > 0 {
		var placeholders []string
		for range stmt.bindings {
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

	return asSQL, nil
}

func (stmt *InsertStmt) Exec() (res sql.Result, err error) {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return res, err
	}

	return stmt.execer.Exec(asSQL, stmt.bindings...)
}

func (stmt *InsertStmt) GetRow(into interface{}) error {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	return sqlx.Get(stmt.execer, into, asSQL, stmt.bindings...)
}

func (stmt *InsertStmt) GetAll(into interface{}) error {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	return sqlx.Select(stmt.execer, into, asSQL, stmt.bindings...)
}
