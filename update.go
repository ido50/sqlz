package sqlz

import (
   "context"
   "database/sql"
   "strings"

   "github.com/jmoiron/sqlx"
)

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
   Table      string
   Updates    map[string]interface{}
   Conditions []WhereCondition
   Return     []string
   execer     Ext
   SelectStmt *SelectStmt
   SelectStmtAlias string
}

// Update creates a new UpdateStmt object for
// the specified table
func (db *DB) Update(table string) *UpdateStmt {
   return &UpdateStmt{
      Table:   table,
      Updates: make(map[string]interface{}),
      execer:  db.DB,
   }
}

// Update creates a new UpdateStmt object for
// the specified table
func (tx *Tx) Update(table string) *UpdateStmt {
   return &UpdateStmt{
      Table:   table,
      Updates: make(map[string]interface{}),
      execer:  tx.Tx,
   }
}

// Set receives the name of a column and a new value. Multiple calls to Set
// can be chained together to modify multiple columns. Set can also be chained
// with calls to SetMap
func (stmt *UpdateStmt) Set(col string, value interface{}) *UpdateStmt {
   return stmt.SetIf(col, value, true)
}

// SetMap receives a map of columns and values. Multiple calls to both Set and
// SetMap can be chained to modify multiple columns.
func (stmt *UpdateStmt) SetMap(updates map[string]interface{}) *UpdateStmt {
   for col, value := range updates {
      stmt.Updates[col] = value
   }
   return stmt
}

// SetIf is the same as Set, but also accepts a boolean value and only does
// anything if that value is true. This is a convenience method so that
// conditional updates can be made without having to save the UpdateStmt into
// a variable and using if statements
func (stmt *UpdateStmt) SetIf(col string, value interface{}, b bool) *UpdateStmt {
   if b {
      stmt.Updates[col] = value
   }
   return stmt
}

// Where creates one or more WHERE conditions for the UPDATE statement.
// If multiple conditions are passed, they are considered AND conditions.
func (stmt *UpdateStmt) Where(conditions ...WhereCondition) *UpdateStmt {
   stmt.Conditions = append(stmt.Conditions, conditions...)
   return stmt
}

// Returning sets a RETURNING clause to receive values back from the
// database once executing the UPDATE statement. Note that GetRow or
// GetAll must be used to execute the query rather than Exec to get
// back the values.
func (stmt *UpdateStmt) Returning(cols ...string) *UpdateStmt {
   stmt.Return = append(stmt.Return, cols...)
   return stmt
}

func (stmt *UpdateStmt) FromSelect(selStmt *SelectStmt,alias string) *UpdateStmt {
   stmt.SelectStmt = selStmt
   stmt.SelectStmtAlias = alias
   return stmt
}

// ToSQL generates the UPDATE statement's SQL and returns a list of
// bindings. It is used internally by Exec, GetRow and GetAll, but is
// exported if you wish to use it directly.
func (stmt *UpdateStmt) ToSQL(rebind bool) (asSQL string, bindings []interface{}) {
   var clauses = []string{"UPDATE " + stmt.Table}

   var updates []string

   for col, val := range stmt.Updates {
      if fn, isFn := val.(UpdateFunction); isFn {
         var args []string
         for _, arg := range fn.Arguments {
            if indirect, isIndirect := arg.(IndirectValue); isIndirect {
               args = append(args, indirect.Reference)
               bindings = append(bindings, indirect.Bindings...)
            } else {
               args = append(args, "?")
               bindings = append(bindings, arg)
            }
         }
         updates = append(updates, col+" = "+fn.Name+"("+strings.Join(args, ", ")+")")
      } else if indirect, isIndirect := val.(IndirectValue); isIndirect {
         updates = append(updates, col+" = "+indirect.Reference)
         bindings = append(bindings, indirect.Bindings...)
      } else {
         updates = append(updates, col+" = ?")
         bindings = append(bindings, val)
      }
   }

   clauses = append(clauses, "SET "+strings.Join(updates, ", "))

   if stmt.SelectStmt != nil && stmt.SelectStmtAlias != ""{
      selectSQL, selectBindings := stmt.SelectStmt.ToSQL(false)
      selectSQL= "("+selectSQL+") AS "+ stmt.SelectStmtAlias+" "
      clauses = append (clauses,"FROM ")
      clauses = append(clauses, selectSQL)
      bindings = append(bindings, selectBindings...)
   }

   if len(stmt.Conditions) > 0 {
      whereClause, whereBindings := parseConditions(stmt.Conditions)
      bindings = append(bindings, whereBindings...)
      clauses = append(clauses, "WHERE "+whereClause)
   }

   if len(stmt.Return) > 0 {
      clauses = append(clauses, "RETURNING "+strings.Join(stmt.Return, ", "))
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

// Exec executes the UPDATE statement, returning the standard
// sql.Result struct and an error if the query failed.
func (stmt *UpdateStmt) Exec() (res sql.Result, err error) {
   asSQL, bindings := stmt.ToSQL(true)
   return stmt.execer.Exec(asSQL, bindings...)
}

// ExecContext executes the UPDATE statement, returning the standard
// sql.Result struct and an error if the query failed.
func (stmt *UpdateStmt) ExecContext(ctx context.Context) (res sql.Result, err error) {
   asSQL, bindings := stmt.ToSQL(true)
   return stmt.execer.ExecContext(ctx, asSQL, bindings...)
}

// GetRow executes an UPDATE statement with a RETURNING clause
// expected to return one row, and loads the result into
// the provided variable (which may be a simple variable if
// only one column is returned, or a struct if multiple columns
// are returned)
func (stmt *UpdateStmt) GetRow(into interface{}) error {
   asSQL, bindings := stmt.ToSQL(true)
   return sqlx.Get(stmt.execer, into, asSQL, bindings...)
}

// GetRowContext executes an UPDATE statement with a RETURNING clause
// expected to return one row, and loads the result into
// the provided variable (which may be a simple variable if
// only one column is returned, or a struct if multiple columns
// are returned)
func (stmt *UpdateStmt) GetRowContext(ctx context.Context, into interface{}) error {
   asSQL, bindings := stmt.ToSQL(true)
   return sqlx.GetContext(ctx, stmt.execer, into, asSQL, bindings...)
}

// GetAll executes an UPDATE statement with a RETURNING clause
// expected to return multiple rows, and loads the result into
// the provided slice variable
func (stmt *UpdateStmt) GetAll(into interface{}) error {
   asSQL, bindings := stmt.ToSQL(true)
   return sqlx.Select(stmt.execer, into, asSQL, bindings...)
}

// GetAllContext executes an UPDATE statement with a RETURNING clause
// expected to return multiple rows, and loads the result into
// the provided slice variable
func (stmt *UpdateStmt) GetAllContext(ctx context.Context, into interface{}) error {
   asSQL, bindings := stmt.ToSQL(true)
   return sqlx.SelectContext(ctx, stmt.execer, into, asSQL, bindings...)
}

// UpdateFunction represents a function call in the context of
// updating a column's value. For example, PostgreSQL provides
// functions to append, prepend or remove items from array
// columns.
type UpdateFunction struct {
   Name      string
   Arguments []interface{}
}

// ArrayAppend is an UpdateFunction for calling PostgreSQL's
// array_append function during an update.
func ArrayAppend(name string, value interface{}) UpdateFunction {
   return UpdateFunction{
      Name:      "array_append",
      Arguments: []interface{}{Indirect(name), value},
   }
}

// ArrayPrepend is an UpdateFunction for calling PostgreSQL's
// array_prepend function during an update.
func ArrayPrepend(name string, value interface{}) UpdateFunction {
   return UpdateFunction{
      Name:      "array_prepend",
      Arguments: []interface{}{Indirect(name), value},
   }
}

// ArrayRemove is an UpdateFunction for calling PostgreSQL's
// array_remove function during an update.
func ArrayRemove(name string, value interface{}) UpdateFunction {
   return UpdateFunction{
      Name:      "array_remove",
      Arguments: []interface{}{Indirect(name), value},
   }
}


