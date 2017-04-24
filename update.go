package sqlz

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
)

type UpdateStmt struct {
	Table      string
	Updates    map[string]interface{}
	Conditions []WhereCondition
	Return     []string
	bindings   []interface{}
	execer     sqlx.Ext
}

func (db *DB) Update(table string) *UpdateStmt {
	return &UpdateStmt{
		Table:   table,
		Updates: make(map[string]interface{}),
		execer:  db.DB,
	}
}

func (tx *Tx) Update(table string) *UpdateStmt {
	return &UpdateStmt{
		Table:  table,
		execer: tx.Tx,
	}
}

func (stmt *UpdateStmt) Bindings() []interface{} {
	return stmt.bindings
}

func (stmt *UpdateStmt) Set(col string, value interface{}) *UpdateStmt {
	stmt.Updates[col] = value
	return stmt
}

func (stmt *UpdateStmt) SetMap(updates map[string]interface{}) *UpdateStmt {
	for col, value := range updates {
		stmt.Updates[col] = value
	}
	return stmt
}

func (stmt *UpdateStmt) Where(conditions ...WhereCondition) *UpdateStmt {
	stmt.Conditions = append(stmt.Conditions, conditions...)
	return stmt
}

func (stmt *UpdateStmt) Returning(cols ...string) *UpdateStmt {
	stmt.Return = append(stmt.Return, cols...)
	return stmt
}

var ErrNoUpdates = errors.New("no updates provided")

func (stmt *UpdateStmt) ToSQL() (asSQL string, err error) {
	if stmt.Table == "" {
		return asSQL, ErrNoTable
	}
	if len(stmt.Updates) == 0 {
		return asSQL, ErrNoUpdates
	}

	stmt.bindings = []interface{}{}

	var clauses = []string{"UPDATE " + stmt.Table}

	var updates []string

	for col, val := range stmt.Updates {
		updates = append(updates, col+" = ?")
		stmt.bindings = append(stmt.bindings, val)
	}

	clauses = append(clauses, "SET "+strings.Join(updates, ", "))

	if len(stmt.Conditions) > 0 {
		whereClause, bindings := parseConditions(stmt.Conditions)
		stmt.bindings = append(stmt.bindings, bindings...)
		clauses = append(clauses, "WHERE "+whereClause)
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

func (stmt *UpdateStmt) Exec() (res sql.Result, err error) {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return res, err
	}

	return stmt.execer.Exec(asSQL, stmt.bindings...)
}

func (stmt *UpdateStmt) GetRow(into interface{}) error {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	return sqlx.Get(stmt.execer, into, asSQL, stmt.bindings...)
}

func (stmt *UpdateStmt) GetAll(into interface{}) error {
	asSQL, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	return sqlx.Select(stmt.execer, into, asSQL, stmt.bindings...)
}
