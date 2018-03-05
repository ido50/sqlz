// Package sqlz implements an SQL query builder based on
// github.com/jmoiron/sqlx.
package sqlz

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// Ext is a union interface which can bind, query, and exec,
// with or without contexts, used by NamedQuery and NamedExec
type Ext interface {
	sqlx.Queryer
	sqlx.QueryerContext
	sqlx.Execer
	sqlx.ExecerContext
}

// Queryer is an interface used by Get and Select, with or without context
type Queryer interface {
	sqlx.Queryer
	sqlx.QueryerContext
}

// DB is a wrapper around sqlx.DB (which is a wrapper around sql.DB)
type DB struct {
	*sqlx.DB
}

// Tx is a wrapper around sqlx.Tx (which is a wrapper around sql.Tx)
type Tx struct {
	*sqlx.Tx
}

// SQLStmt is an interface representing a general SQL statement. All
// specific statement types (e.g. SelectStmt, UpdateStmt, etc.)
// implement this interface
type SQLStmt interface {
	ToSQL(bool) (string, []interface{})
}

type SQLSimpleClause interface{
   ToSQL()string
}

// ToSQL generates SQL for an IndirectValue
func (i IndirectValue) ToSQL() string{
	return i.Reference
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
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed committing transaction: %s", err)
	}

	return nil
}

// TransactionalContext runs the provided function inside a transaction. The
// function must receive an sqlz Tx object, and return an error. If the
// function returns an error, the transaction is automatically rolled
// back. Otherwise, the transaction is committed.
func (db *DB) TransactionalContext(ctx context.Context, opts *sql.TxOptions, f func(tx *Tx) error) error {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed starting transaction: %s", err)
	}

	err = f(&Tx{tx})
	if err != nil {
		tx.Rollback()
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

// SQLCondition represents a condition written directly in
// SQL, allows using complex SQL conditions not yet supported
// by sqlz
type SQLCondition struct {
	Condition string
	Binds     []interface{}
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

// SQLCond creates an SQL condition, allowing to use complex SQL conditions
// that are not yet supported by sqlz. Question marks must be used for
// placeholders in the condition regardless of the database driver.
func SQLCond(condition string, binds ...interface{}) SQLCondition {
	return SQLCondition{condition, binds}
}

// InCondition is a struct representing IN and NOT IN conditions
type InCondition struct {
	NotIn bool
	Left  string
	Right []interface{}
}

// In creates an IN condition for matching the value of a column
// against an array of possible values
func In(col string, values ...interface{}) InCondition {
	return InCondition{false, col, values}
}

// NotIn creates a NOT IN condition for checking that the value
// of a column is not one of the defined values
func NotIn(col string, values ...interface{}) InCondition {
	return InCondition{true, col, values}
}

// ArrayCondition represents an array comparison condition
type ArrayCondition struct {
	Left     interface{}
	Operator string
	Type     string
	Right    string
}

// EqAny creates an "= ANY" condition on an array column
func EqAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "=", "ANY", col}
}

// NeAny creates an "<> ANY" condition on an array column
func NeAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<>", "ANY", col}
}

// LtAny creates an "< ANY" condition on an array column
func LtAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<", "ANY", col}
}

// LteAny creates an "<= ANY" condition on an array column
func LteAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<=", "ANY", col}
}

// GtAny creates an "> ANY" condition on an array column
func GtAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">", "ANY", col}
}

// GteAny creates an ">= ANY" condition on an array column
func GteAny(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">=", "ANY", col}
}

// EqAll creates an "= ALL" condition on an array column
func EqAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "=", "ALL", col}
}

// NeAll creates an "<> ALL" condition on an array column
func NeAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<>", "ALL", col}
}

// LtAll creates an "< ALL" condition on an array column
func LtAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<", "ALL", col}
}

// LteAll creates an "<= ALL" condition on an array column
func LteAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<=", "ALL", col}
}

// GtAll creates an "> ALL" condition on an array column
func GtAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">", "ALL", col}
}

// GteAll creates an ">= ALL" condition on an array column
func GteAll(col string, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">=", "ALL", col}
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
func (cond SQLCondition) Parse() (asSQL string, bindings []interface{}) {
	return cond.Condition, cond.Binds
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (array ArrayCondition) Parse() (asSQL string, bindings []interface{}) {
	if indirect, isIndirect := array.Left.(IndirectValue); isIndirect {
		asSQL = indirect.Reference
	} else {
		asSQL = "?"
		bindings = append(bindings, array.Left)
	}
	asSQL += " " + array.Operator + " " + array.Type + "(" + array.Right + ")"

	return asSQL, bindings
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (in InCondition) Parse() (asSQL string, bindings []interface{}) {
	asSQL = in.Left
	if in.NotIn {
		asSQL += " NOT"
	}
	asSQL += " IN ("

	var placeholders []string

	for _, val := range in.Right {
		placeholders = append(placeholders, "?")
		bindings = append(bindings, val)
	}

	asSQL += strings.Join(placeholders, ", ") + ")"

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
