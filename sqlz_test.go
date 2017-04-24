package sqlz

import (
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type toSQLer interface {
	ToSQL() (string, error)
	Bindings() []interface{}
}

type test struct {
	name     string
	stmt     toSQLer
	expected string
	bindings []interface{}
}

func runTests(t *testing.T, source func(dbz *DB) []test) {
	dbx, err := sqlx.Connect("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed connecting to DB: %s", err)
	}

	for _, tst := range source(Newx(dbx)) {
		sql, err := tst.stmt.ToSQL()
		if err != nil {
			t.Errorf("Failed %s: %s", tst.name, err)
		} else if sql != tst.expected {
			t.Errorf("Failed %s: expected %s, got %s", tst.name, tst.expected, sql)
		}

		if len(tst.bindings) != len(tst.stmt.Bindings()) {
			t.Errorf("Failed %s: expected %d bindings, got %d", tst.name, len(tst.bindings), len(tst.stmt.Bindings()))
		} else {
			for i := range tst.bindings {
				if tst.bindings[i] != tst.stmt.Bindings()[i] {
					t.Errorf("Failed %s: expected binding %d to be %v, got %v", tst.name, i+1, tst.bindings[i], tst.stmt.Bindings()[i])
				}
			}
		}
	}
}
