// Package sqlz implements an SQL query builder based on
// github.com/jmoiron/sqlx.
package sqlz

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// DB is a wrapper around sqlx.DB (which is a wrapper around sql.DB)
type DB struct {
	*sqlx.DB
}

// Tx is a wrapper around sqlx.Tx (which is a wrapper around sql.Tx)
type Tx struct {
	*sqlx.Tx
}

// New creates a new DB instance from an underlying sql.DB object.
// It requires the name of the SQL driver in order to use the correct
// placeholders when generating SQL
func New(db *sql.DB, driverName string) *DB {
	return &DB{DB: sqlx.NewDb(db, driverName)}
}

// Newx creates a new DB instance from an underlying sqlx.DB object
func Newx(db *sqlx.DB) *DB {
	return &DB{DB: db}
}

// Transactional runs the provided function inside a transaction. The
// function must receive an sqlz Tx object, and return an error. If the
// function returns an error, the transaction is automatically rolled
// back. Otherwise, the transaction is committed.
func (db *DB) Transactional(f func(tx *Tx) error) error {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed starting transaction: %s", err)
	}

	err = f(&Tx{tx})
	if err != nil {
		rErr := tx.Rollback()
		err = fmt.Errorf("transaction failed: %s", err)
		if rErr != nil {
			err = fmt.Errorf("%s (rollback failed: %s)", err, rErr)
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed committing transaction: %s", err)
	}

	return nil
}

// WhereCondition is an interface describing conditions
// that can be used inside an SQL WHERE clause. It defines
// the Parse function that generates SQL (with placeholders)
// from the condition(s) and returns a list of data bindings
// for the placeholders (if any)
type WhereCondition interface {
	Parse() (asSQL string, bindings []interface{})
}

// SimpleCondition represents the most basic WHERE
// condition, where one left-value (usually a column)
// is compared with a right-value using an operator (e.g.
// "=", "<>", ">=", ...)
type SimpleCondition struct {
	Left     string
	Right    interface{}
	Operator string
}

// AndOrCondition represents a group of AND or OR
// conditions.
type AndOrCondition struct {
	Or         bool
	Conditions []WhereCondition
}

// SubqueryCondition is a WHERE condition on the results
// of a sub-query.
type SubqueryCondition struct {
	Stmt     *SelectStmt
	Operator string
}

// IndirectValue represents a reference to a database name
// (e.g. column, function) that should be used as-is in a
// query rather than replaced with a placeholder.
type IndirectValue struct {
	Reference string
}

// Indirect receives a string and injects it into a query
// as-is rather than with a placeholder. Use this when
// comparing columns, modifying columns based on their (or
// others') existing values, using database functions, etc.
// Never use this with user-supplied input, as this may
// open the door for SQL injections!
func Indirect(value string) IndirectValue {
	return IndirectValue{value}
}

// And joins multiple where conditions as an AndOrCondition
// (representing AND conditions). You will use this a lot
// less than Or as passing multiple conditions to functions
// like Where or Having are all AND conditions.
func And(conds ...WhereCondition) AndOrCondition {
	return AndOrCondition{false, conds}
}

// Or joins multiple where conditions as an AndOrCondition
// (representing OR conditions).
func Or(conds ...WhereCondition) AndOrCondition {
	return AndOrCondition{true, conds}
}

// Eq represents a simple equality condition ("=" operator)
func Eq(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "="}
}

// Ne represents a simple non-equality condition ("<>" operator)
func Ne(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<>"}
}

// Gt represents a simple greater-than condition (">" operator)
func Gt(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, ">"}
}

// Gte represents a simple greater-than-or-equals condition (">=" operator)
func Gte(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, ">="}
}

// Lt represents a simple less-than condition ("<" operator)
func Lt(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<"}
}

// Lte represents a simple less-than-or-equals condition ("<=" operator)
func Lte(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "<="}
}

// Like represents a wildcard equality condition ("LIKE" operator)
func Like(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "LIKE"}
}

// NotLike represents a wildcard non-equality condition ("NOT LIKE" operator)
func NotLike(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "NOT LIKE"}
}

// IsNull represents a simple nullity condition ("IS NULL" operator)
func IsNull(col string) SimpleCondition {
	return SimpleCondition{col, nil, "IS NULL"}
}

// IsNotNull represents a simple non-nullity condition ("IS NOT NULL" operator)
func IsNotNull(col string) SimpleCondition {
	return SimpleCondition{col, nil, "IS NOT NULL"}
}

// Exists creates a sub-query condition checking the sub-query
// returns results ("EXISTS" operator)
func Exists(stmt *SelectStmt) SubqueryCondition {
	return SubqueryCondition{stmt, "EXISTS"}
}

// NotExists creates a sub-query condition checking the sub-query
// does not return results ("NOT EXISTS" operator)
func NotExists(stmt *SelectStmt) SubqueryCondition {
	return SubqueryCondition{stmt, "NOT EXISTS"}
}

// JSONBOp creates simple conditions with JSONB operators for
// PostgreSQL databases (supported operators are "@>", "<@",
// "?", "?!", "?&", "||", "-" and "#-")
func JSONBOp(op string, left string, value interface{}) SimpleCondition {
	switch op {
	case "@>", "<@", "?", "?!", "?&", "||", "-", "#-":
		return SimpleCondition{left, value, op}
	default:
		return SimpleCondition{}
	}
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
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

// Parse implements the WhereCondition interface, generating SQL from
// the condition
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

// Parse implements the WhereCondition interface, generating SQL from
// the condition
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

	if strings.HasPrefix(asSQL, "(") {
		asSQL = strings.TrimPrefix(strings.TrimSuffix(asSQL, ")"), "(")
	}

	return asSQL, bindings
}
