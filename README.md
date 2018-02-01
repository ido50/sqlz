<h2 align="center">sqlz</h2>
<p align="center">sqlz is an SQL query builder for Go.</p>
<p align="center">
	<a href="https://godoc.org/github.com/ido50/sqlz"><img src="https://img.shields.io/badge/godoc-reference-blue.svg"></a>
    <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg"></a>
	<a href="https://goreportcard.com/report/ido50/sqlz"><img src="https://goreportcard.com/badge/github.com/ido50/sqlz"></a>
</p>

---

sqlz is an un-opinionated, un-obtrusive SQL query builder for Go projects, based on [sqlx](https://github.com/jmoiron/sqlx/).

As opposed to other query builders, sqlz does not mean to bridge the gap between different SQL servers and implementations by
providing a unified interface. Instead, it aims to support an extended SQL syntax that may be implementation-specific. For
example, if you wish to use PostgreSQL-specific features such as JSON operators and upsert statements, sqlz means to support
these without caring if the underlying database backend really is PostgreSQL. In other words, sqlz builds whatever queries
you want it to build.

sqlz is easy to integrate into existing code, as it does not require you to create your database connections through the
sqlz API; in fact, it doesn't supply one. You can either use your existing `*sql.DB` connection or an `*sqlx.DB` connection,
so you can start writing new queries with sqlz without having to modify any existing code.

sqlz leverages sqlx for easy loading of query results. Please make sure you are familiar with [how sqlx works](https://jmoiron.github.io/sqlx/)
in order to understand how row scanning is performed. You may need to add `db` struct tags to your Go structures.

sqlz provides a comfortable API for running queries in a transaction, and will automatically commit or rollback the
transaction as necessary.

**NOTE 1**: "sqlz" is currently a working name, and may change soon.

**NOTE 2**: sqlz is in an early stage, currently mostly targeting PostgreSQL. There's much more work to do, but it's
definitely usable. API may change, though I'm not currently planning on doing so.

## Install

To install sqlz globally:

```go
go get -u github.com/ido50/sqlz
```

Alternatively, use your preferred Go depedency manager to vendor sqlz into your projects.

## Usage

Once installed, you can import sqlz into your Go packages. To build and execute queries with
sqlz, you need to pass the underlying `*sql.DB` or `*sqlx.DB` objects. If using `database/sql`,
you'll need to tell sqlz the name of the driver (so that it knows which placeholders to use
when building queries); if using `jmoiron/sqlx`, this is not necessary.

```go
package main

import (
    "fmt"
    "database/sql"
    "github.com/ido50/sqlz"
)

func main() {
    driver := "postgres"

    db, err := sql.Open(driver, "dsn")
    if err != nil {
        panic(err)
    }

    // find one row in the database and load it
    // into a struct variable
    var row someStruct
    err = sqlz.New(db, driver).  // if using sqlx: sqlz.Newx(dbx)
        Select("*").
        From("some-table").
        Where(sqlz.Eq("id", 1)).
        GetRow(&row)
    if err != nil {
        panic(err)
    }

    fmt.Printf("%+v\n", row)
}
```

## Examples

### Load one row from a table

```go
var row someStruct
err = sqlz.New(db, driver).
    Select("*").
    From("some-table").
    Where(Sqlz.Eq("id", 1)).
    GetRow(&row)
```

Generated SQL (disregarding placeholders):

```sql
   SELECT *
     FROM some-table
    WHERE id = 1
```

### Complex load of many rows with pagination

```go
var rows []struct{
    maxVal int64
    sumCount uint64
}

err = sqlz.New(db, driver).
     Select("MAX(t.col1) maxVal", "SUM(t.col2) sumCount").
     From("some-table t").
     LeftJoin("other-table o", sqlz.Eq("o.id", sqlz.Indirect("t.id"))). // there's also RightJoin, InnerJoin, FullJoin
     GroupBy("t.col3", "t.col4").
     Having(sqlz.Gte("maxVal", 3)).
     OrderBy(sqlz.Desc("maxVal"), sqlz.Asc("sumCount")).
     Limit(5).
     Offset(10).
     Where(sqlz.Or(sqlz.Eq("t.col3", 5), sqlz.IsNotNull("t.col4"))).
     GetAll(&rows)
```

Generated SQL (disregarding placeholders):

```sql
        SELECT MAX(t.col1) maxVal, SUM(t.col2) sumCount
        FROM some-table t
   LEFT JOIN other-table o ON o.id = t.id
       WHERE t.col3 = 5 OR t.col4 IS NOT NULL
    GROUP BY t.col3, t.col4
      HAVING maxVal > 3
    ORDER BY maxVal DESC, sumCount ASC
       LIMIT 5
      OFFSET 10, 20
```

When paginating results, sqlz provides a nice feature to also calculate the
total number of results matching the query, regardless of limiting and offsets:

```go
var rows []struct{
    maxVal int64
    sumCount uint64
}

query := sqlz.New(db, driver).
     Select("MAX(t.col1) maxVal", "SUM(t.col2) sumCount").
     // rest of the query as before
count, err := query.GetCount() // returns total number of results available, regardless of limits and offsets
err = query.GetAll(&rows)      // returns actual results according to limits and offsets
```

### Simple inserts

```go
res, err := sqlz.New(db, driver).
    InsertInto("table").
    Columns("id", "name").
    Values(1, "My Name").
    Exec()

// res is sql.Result
```

Generated SQL:

```sql
INSERT INTO table (id, name) VALUES (?, ?)
```

### Inserts with a value map

```go
res, err := sqlz.New(db, driver).
    InsertInto("table").
    ValueMap(map[string]interface{}{
        "id": 1,
        "name": "My Name",
    }).
    Exec()
```

Generates the same SQL as for [simple inserts](#simple-inserts).

### Inserts returning values

```go
var id int64
err := sqlz.New(db, driver).
    InsertInto("table").
    Columns("name").
    Values("My Name").
    Returning("id").
    GetRow(&id)
```

### Update rows

```go
res, err := sqlz.New(db, driver).
    Update("table").
    Set("col1", "some-string").
    SetMap(map[string]interface{}{
        "col2": true,
        "col3": 5,
    }).
    Where(sqlz.Eq("id", 3)).
    Exec()

```

Generated SQL:

```sql
   UPDATE table
      SET col1 = ?, col2 = ?, col3 = ?
    WHERE id = ?
```

Updates support the RETURNING clause just like inserts.

### Delete rows

```go
res, err := sqlz.New(db, driver).
    DeleteFrom("table").
    Where(sqlz.Eq("id", 3)).
    Exec()
```

Generated SQL:

```sql
   DELETE FROM table
         WHERE id = ?
```

### Easy transactions

sqlz makes it easy to run multiple queries in a transaction, and will automatically rollback or commit as necessary:

```go
sqlz.
    New(db, driver).
    Transactional(func(tx *sqlz.Tx) error {
        var id int64
        err := tx.InsertInto("table").Columns("name").Values("some guy").GetRow(&id)
        if err != nil {
            return err
        }

        _, err = tx.Update("other-table").Set("some-col", 4).Exec()
        if err != nil {
            return err
        }

        return nil
    })
```

If the function provided to the Transactional method returns an error, the transaction
will be rolled back. Otherwise, it will be committed.

### Using strings as-is in queries

If you need to compare columns, call database functions, modify columns based on their
(or other's) existing values, and any place you need strings to be used as-is and not
replaced with placeholders, use the Indirect function:

 - To compare two columns in a WHERE clause, use `sqlz.Eq("column-one", sqlz.Indirect("column-two"))`
 - To increase a column in a SET clause, use `sqlz.Set("int-column", sqlz.Indirect("int-column + 1"))`
 - To set a columm using a database function (e.g. `LOCALTIMESTAMP`), use `sqlz.Set("datetime", sqlz.Indirect("LOCALTIMESTAMP"))`

## Dependencies

The only non-standard library package used is [jmoiron/sqlx](https://github.com/jmoiron/sqlx).
The test suite, however, uses [DATA-DOG/sqlmock](https://github.com/DATA-DOG/sqlmock).

## Acknowledgments

sqlz was inspired by [gocraft/dbr](https://github.com/gocraft/dbr).
