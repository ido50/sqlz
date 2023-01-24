package sqlz

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
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
	ErrHandlers []func(err error)
}

// Tx is a wrapper around sqlx.Tx (which is a wrapper around sql.Tx)
type Tx struct {
	*sqlx.Tx
	ErrHandlers []func(err error)
}

// SQLStmt is an interface representing a general SQL statement. All
// specific statement types (e.g. SelectStmt, UpdateStmt, etc.)
// implement this interface
type SQLStmt interface {
	ToSQL(bool) (string, []interface{})
}

// New creates a new DB instance from an underlying sql.DB object.
// It requires the name of the SQL driver in order to use the correct
// placeholders when generating SQL
func New(db *sql.DB, driverName string, errHandlerFuncs ...func(err error)) *DB {
	errHandlers := make([]func(err error), len(errHandlerFuncs))
	copy(errHandlers, errHandlerFuncs)

	return &DB{
		DB:          sqlx.NewDb(db, driverName),
		ErrHandlers: errHandlers,
	}
}

// Newx creates a new DB instance from an underlying sqlx.DB object
func Newx(db *sqlx.DB) *DB {
	return &DB{DB: db}
}

// Transactional runs the provided function inside a transaction. The
// function must receive an sqlz Tx object, and return an error. If the
// function returns an error, the transaction is automatically rolled
// back. Otherwise, the transaction is committed.
func (db *DB) Transactional(f func(tx *Tx) error, opts ...*sql.TxOptions) error {
	var lastOpts *sql.TxOptions
	if len(opts) > 0 {
		lastOpts = opts[len(opts)-1]
	}

	return db.TransactionalContext(context.Background(), lastOpts, f)
}

// TransactionalContext runs the provided function inside a transaction. The
// function must receive an sqlz Tx object, and return an error. If the
// function returns an error, the transaction is automatically rolled
// back. Otherwise, the transaction is committed.
func (db *DB) TransactionalContext(
	ctx context.Context,
	opts *sql.TxOptions,
	f func(tx *Tx) error,
) error {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed starting transaction: %w", err)
	}

	err = f(&Tx{Tx: tx, ErrHandlers: db.ErrHandlers})
	if err != nil {
		tx.Rollback() //nolint: errcheck
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed committing transaction: %w", err)
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

// PreCondition represents pre-condition operator
type PreCondition struct {
	Pre       string
	Condition WhereCondition
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
	Bindings  []interface{}
}

// Indirect receives a string and injects it into a query
// as-is rather than with a placeholder. Use this when
// comparing columns, modifying columns based on their (or
// others') existing values, using database functions, etc.
// Never use this with user-supplied input, as this may
// open the door for SQL injections!
func Indirect(value string, bindings ...interface{}) IndirectValue {
	return IndirectValue{value, bindings}
}

// ToSQL returns the indirect value as SQL, together with its bindings.
func (i IndirectValue) ToSQL(_ bool) (string, []interface{}) {
	return i.Reference, i.Bindings
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

// Not represents a pre condition ("NOT" operator)
func Not(cond WhereCondition) PreCondition {
	return PreCondition{"NOT", cond}
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

// ILike represents a wildcard equality condition ("ILIKE" operator)
func ILike(col string, value interface{}) SimpleCondition {
	return SimpleCondition{col, value, "ILIKE"}
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
	Right    interface{}
}

// Any creates an "ANY (array)" condition, to lookup for a value matching against an array of possible values
// as similar to IN condition
func Any(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{arr, "=", "ANY", value}
}

// EqAny creates an "= ANY" condition on an array column
func EqAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "=", "ANY", arr}
}

// NeAny creates an "<> ANY" condition on an array
func NeAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<>", "ANY", arr}
}

// LtAny creates an "< ANY" condition on an array
func LtAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<", "ANY", arr}
}

// LteAny creates an "<= ANY" condition on an array
func LteAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<=", "ANY", arr}
}

// GtAny creates an "> ANY" condition on an array
func GtAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">", "ANY", arr}
}

// GteAny creates an ">= ANY" condition on an array
func GteAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">=", "ANY", arr}
}

// EqAll creates an "= ALL" condition on an array
func EqAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "=", "ALL", arr}
}

// NeAll creates an "<> ALL" condition on an array
func NeAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<>", "ALL", arr}
}

// LtAll creates an "< ALL" condition on an array
func LtAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<", "ALL", arr}
}

// LteAll creates an "<= ALL" condition on an array
func LteAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "<=", "ALL", arr}
}

// GtAll creates an "> ALL" condition on an array
func GtAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">", "ALL", arr}
}

// GteAll creates an ">= ALL" condition on an array
func GteAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, ">=", "ALL", arr}
}

// LikeAny creates an "Like ANY" condition on an array
func LikeAny(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "LIKE", "ANY", arr}
}

func NotLikeAll(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "NOT LIKE", "ALL", arr}
}

func NotLikeANY(arr interface{}, value interface{}) ArrayCondition {
	return ArrayCondition{value, "NOT LIKE", "ANY", arr}
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (simple SimpleCondition) Parse() (asSQL string, bindings []interface{}) {
	asSQL = simple.Left + " " + simple.Operator

	if simple.Right != nil {
		placeholder := "?"
		if indirect, isIndirect := simple.Right.(IndirectValue); isIndirect {
			placeholder = indirect.Reference
			bindings = append(bindings, indirect.Bindings...)
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
	rightAsSQL := "?"
	leftAsSQL := "?"

	if indirect, isIndirect := array.Left.(IndirectValue); isIndirect {
		leftAsSQL = indirect.Reference
		bindings = append(bindings, indirect.Bindings...)
	} else {
		bindings = append(bindings, array.Left)
	}

	switch right := array.Right.(type) {
	case string:
		rightAsSQL = fmt.Sprintf("%v", array.Right)
	case []int:
		var values []string
		for _, n := range right {
			values = append(values, strconv.Itoa(n))
		}

		binds := fmt.Sprintf("'{%s}'", strings.Join(values, ","))
		bindings = append(bindings, binds)
	default:
		bindings = append(bindings, array.Right)
	}

	return fmt.Sprintf(
		"%s %s %s(%s)",
		leftAsSQL, array.Operator, array.Type, rightAsSQL,
	), bindings
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (in InCondition) Parse() (asSQL string, bindings []interface{}) {
	asSQL = in.Left
	if in.NotIn {
		asSQL += " NOT"
	}

	asSQL += " IN ("

	placeholders := make([]string, len(in.Right))
	for i, val := range in.Right {
		placeholders[i] = "?"

		bindings = append(bindings, val)
	}

	asSQL += strings.Join(placeholders, ", ") + ")"

	return asSQL, bindings
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (andOr AndOrCondition) Parse() (asSQL string, bindings []interface{}) {
	sqls := make([]string, len(andOr.Conditions))

	for i, cond := range andOr.Conditions {
		innerSQL, innerBindings := cond.Parse()
		sqls[i] = innerSQL

		bindings = append(bindings, innerBindings...)
	}

	op := " AND "
	if andOr.Or {
		op = " OR "
	}

	return fmt.Sprintf("(%s)", strings.Join(sqls, op)), bindings
}

// Parse implements the WhereCondition interface, generating SQL from
// the condition
func (pre PreCondition) Parse() (asSQL string, bindings []interface{}) {
	innerSQL, innerBindings := pre.Condition.Parse()
	bindings = append(bindings, innerBindings...)

	return fmt.Sprintf("%s(%s)", pre.Pre, innerSQL), bindings
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

func sortKeys(m map[string]interface{}) []string {
	var i int

	keys := make([]string, len(m))
	for key := range m {
		keys[i] = key
		i++
	}

	sort.Strings(keys)

	return keys
}
