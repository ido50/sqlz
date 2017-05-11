package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

type UpdateStmt struct {
	Table      string
	Updates    map[string]interface{}
	Conditions []WhereCondition
	Return     []string
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
		Table:   table,
		Updates: make(map[string]interface{}),
		execer:  tx.Tx,
	}
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

func (stmt *UpdateStmt) ToSQL(_ bool) (asSQL string, bindings []interface{}) {
	var clauses = []string{"UPDATE " + stmt.Table}

	var updates []string

	for col, val := range stmt.Updates {
		if indirect, isIndirect := val.(IndirectValue); isIndirect {
			updates = append(updates, col+" = "+indirect.Reference)
		} else {
			updates = append(updates, col+" = ?")
			bindings = append(bindings, val)
		}
	}

	clauses = append(clauses, "SET "+strings.Join(updates, ", "))

	if len(stmt.Conditions) > 0 {
		whereClause, whereBindings := parseConditions(stmt.Conditions)
		bindings = append(bindings, whereBindings...)
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

	return asSQL, bindings
}

func (stmt *UpdateStmt) Exec() (res sql.Result, err error) {
	asSQL, bindings := stmt.ToSQL(true)
	return stmt.execer.Exec(asSQL, bindings...)
}

func (stmt *UpdateStmt) GetRow(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Get(stmt.execer, into, asSQL, bindings...)
}

func (stmt *UpdateStmt) GetAll(into interface{}) error {
	asSQL, bindings := stmt.ToSQL(true)
	return sqlx.Select(stmt.execer, into, asSQL, bindings...)
}
