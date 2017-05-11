package sqlz

import (
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type toSQLer interface {
	ToSQL(bool) (string, []interface{})
}

type test struct {
	name             string
	stmt             toSQLer
	expectedSQL      string
	expectedBindings []interface{}
}

func runTests(t *testing.T, source func(dbz *DB) []test) {
	dbx, err := sqlx.Connect("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed connecting to DB: %s", err)
	}

	for _, tst := range source(Newx(dbx)) {
		resultingSQL, resultingBindings := tst.stmt.ToSQL(true)
		if resultingSQL != tst.expectedSQL {
			t.Errorf("Failed %s: expected %s, got %s", tst.name, tst.expectedSQL, resultingSQL)
		}

		if len(tst.expectedBindings) != len(resultingBindings) {
			t.Errorf("Failed %s: expected %d bindings, got %d", tst.name, len(tst.expectedBindings), len(resultingBindings))
		} else {
			for i := range tst.expectedBindings {
				if tst.expectedBindings[i] != resultingBindings[i] {
					t.Errorf("Failed %s: expected binding %d to be %v, got %v", tst.name, i+1, tst.expectedBindings[i], resultingBindings[i])
				}
			}
		}
	}
}
