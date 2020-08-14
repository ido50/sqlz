// Package sqlz (pronounced "sequelize") is an un-opinionated, un-obtrusive SQL
// query builder for Go projects, based on github.com/jmoiron/sqlx.
//
// As opposed to other query builders, sqlz does not mean to bridge the gap
// between different SQL servers and implementations by providing a unified
// interface. Instead, it aims to support an extended SQL syntax that may be
// implementation-specific. For example, if you wish to use PostgreSQL-specific
// features such as JSON operators and upsert statements, sqlz means to support
// these without caring if the underlying database backend really is PostgreSQL.
// In other words, sqlz builds whatever queries you want it to build.
//
// sqlz is easy to integrate into existing code, as it does not require you to
// create your database connections through the sqlz API; in fact, it doesn't
// supply one. You can either use your existing `*sql.DB` connection or an
// `*sqlx.DB` connection, so you can start writing new queries with sqlz without
// having to modify any existing code.
//
// sqlz leverages sqlx for easy loading of query results. Please make sure you
// are familiar with how sqlx works in order to understand how row scanning is
// performed. You may need to add `db` struct tags to your Go structures.
//
// sqlz provides a comfortable API for running queries in a transaction, and
// will automatically commit or rollback the transaction as necessary.
//
//		import (
//			"fmt"
//			"database/sql"
//			"github.com/ido50/sqlz"
//			_ "sql driver of choice"
//		)
//
//		func main() {
//			db, err := sql.Open(driver, "dsn")
//			if err != nil {
//				panic(err)
//			}
//
//			// find one row in the database and load it
//			// into a struct variable
//			var row someStruct
//			err = sqlz.New(db, driver).  // if using sqlx: sqlz.Newx(dbx)
//				Select("*").
//				From("some-table").
//				Where(sqlz.Eq("id", 1)).
//				GetRow(&row)
//			if err != nil {
//				panic(err)
//			}
//
//			fmt.Printf("%+v\n", row)
//		}
package sqlz
