package sqlz

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB
}

type Tx struct {
	*sqlx.Tx
}

func New(db *sql.DB, driverName string) *DB {
	return &DB{DB: sqlx.NewDb(db, driverName)}
}

func Newx(db *sqlx.DB) *DB {
	return &DB{DB: db}
}

func (db *DB) Transactional(f func(tx *Tx) error) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	err = f(&Tx{tx})
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

type WhereCondition interface {
	Parse() (asSQL string, bindings []interface{})
}

type AndOrCondition struct {
	Or         bool
	Conditions []WhereCondition
}

type SimpleCondition struct {
	Left     string
	Right    interface{}
	Operator string
}

type SubqueryCondition struct {
	Stmt     *SelectStmt
	Operator string
}

type IndirectValue struct {
	Reference string
}

func Indirect(value string) IndirectValue {
	return IndirectValue{value}
}

func And(conds ...WhereCondition) AndOrCondition {
	return AndOrCondition{false, conds}
}

func Or(conds ...WhereCondition) AndOrCondition {
	return AndOrCondition{true, conds}
}

func Eq(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "="}
}

func Ne(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<>"}
}

func Gt(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, ">"}
}

func Gte(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, ">="}
}

func Lt(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<"}
}

func Lte(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<="}
}

func Like(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "LIKE"}
}

func NotLike(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "NOT LIKE"}
}

func IsNull(col string) SimpleCondition {
	return SimpleCondition{col, nil, "IS NULL"}
}

func IsNotNull(col string) SimpleCondition {
	return SimpleCondition{col, nil, "IS NOT NULL"}
}

func Exists(stmt *SelectStmt) SubqueryCondition {
	return SubqueryCondition{stmt, "EXISTS"}
}

func NotExists(stmt *SelectStmt) SubqueryCondition {
	return SubqueryCondition{stmt, "NOT EXISTS"}
}

func (simple SimpleCondition) Parse() (asSQL string, bindings []interface{}) {
	asSQL = simple.Left + " " + simple.Operator

	if simple.Right != nil {
		placeholder := "?"
		if indirect, isIndirect := simple.Right.(IndirectValue); isIndirect {
			placeholder = indirect.Reference
		} else {
			bindings = append(bindings, simple.Right)
		}
		asSQL += " " + placeholder
	}

	return asSQL, bindings
}

func (andOr AndOrCondition) Parse() (asSQL string, bindings []interface{}) {
	var sqls []string
	for _, cond := range andOr.Conditions {
		innerSQL, innerBindings := cond.Parse()
		sqls = append(sqls, innerSQL)
		bindings = append(bindings, innerBindings...)
	}
	op := " AND "
	if andOr.Or {
		op = " OR "
	}
	return "(" + strings.Join(sqls, op) + ")", bindings
}

func (subCond SubqueryCondition) Parse() (asSQL string, bindings []interface{}) {
	asSQL, bindings = subCond.Stmt.ToSQL(false)
	return subCond.Operator + " (" + asSQL + ")", bindings
}

func parseConditions(conds []WhereCondition) (asSQL string, bindings []interface{}) {
	if len(conds) > 1 {
		asSQL, bindings = (AndOrCondition{false, conds}).Parse()
	} else if len(conds) == 1 {
		asSQL, bindings = conds[0].Parse()
	}

	return strings.TrimPrefix(strings.TrimSuffix(asSQL, ")"), "("), bindings
}
