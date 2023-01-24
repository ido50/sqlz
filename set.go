package sqlz

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// SetCmd represents a PostgreSQL SET command
type SetCmd struct {
	*Statement
	level       string
	configParam string
	value       string
	execer      Ext
}

// Set creates a new SetCmd object, with configuration parameter
// and its value.
func (db *DB) Set(configParam, value string) *SetCmd {
	return &SetCmd{
		configParam: configParam,
		value:       value,
		execer:      db.DB,
		Statement:   &Statement{db.ErrHandlers},
	}
}

// Set creates a new SetCmd object, with configuration parameter
// and its value.
func (tx *Tx) Set(configParam, value string) *SetCmd {
	return &SetCmd{
		configParam: configParam,
		value:       value,
		execer:      tx.Tx,
		Statement:   &Statement{tx.ErrHandlers},
	}
}

// SetTimeout sets a statement timeout. When set, any statement
// (in the transaction) that takes more than the specified duration
// will be aborted, starting from the time the command arrives
// at the server from the client. A value of zero turns this off.
func (tx *Tx) SetTimeout(d time.Duration) (res sql.Result, err error) {
	stmt := &SetCmd{
		configParam: "statement_timeout",
		value:       fmt.Sprintf("\"%dms\"", d.Milliseconds()),
		execer:      tx.Tx,
		Statement:   &Statement{tx.ErrHandlers},
	}

	return stmt.Local().Exec()
}

// Local sets the configuration parameter locally in a transaction.
//
//	The effect of SET LOCAL will last only till the end of the
//
// current transaction, whether committed or not.
func (cmd *SetCmd) Local() *SetCmd {
	cmd.level = "LOCAL"
	return cmd
}

// Session sets the configuration parameter to the entire session.
//
//	The effect of SET SESSION will last only till the end of the
//
// current session. if issued within a transaction that is later aborted,
// the effects of the SET command disappear when the transaction is rolled
// back. Once the surrounding transaction is committed, the effects will persist
// until the end of the session, unless overridden by another SET.
func (cmd *SetCmd) Session() *SetCmd {
	cmd.level = "SESSION"
	return cmd
}

// ToSQL generates the SET command SQL and returns a list of
// bindings. It is used internally by Exec, but is exported if you
// wish to use it directly.
func (cmd *SetCmd) ToSQL(rebind bool) (string, []interface{}) {
	clauses := []string{"SET"}
	if cmd.level != "" {
		clauses = append(clauses, cmd.level)
	}

	clauses = append(clauses, cmd.configParam, "TO", cmd.value)

	asSQL := strings.Join(clauses, " ")

	if rebind {
		if db, ok := cmd.execer.(*sqlx.DB); ok {
			asSQL = db.Rebind(asSQL)
		} else if tx, ok := cmd.execer.(*sqlx.Tx); ok {
			asSQL = tx.Rebind(asSQL)
		}
	}

	return asSQL, []interface{}{}
}

// Exec executes the SET command, returning the standard
// sql.Result struct and an error if the query failed.
func (cmd *SetCmd) Exec() (res sql.Result, err error) {
	asSQL, bindings := cmd.ToSQL(true)
	res, err = cmd.execer.Exec(asSQL, bindings...)
	cmd.Statement.HandleError(err)

	return res, err
}
