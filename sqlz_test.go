package sqlz

import (
	"testing"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type test struct {
	name             string
	stmt             SQLStmt
	expectedSQL      string
	expectedBindings []interface{}
}

func runTests(t *testing.T, source func(dbz *DB) []test) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed creating mock database: %s", err)
	}

	for _, tst := range source(New(db, "sqlmock")) {
		t.Run(tst.name, func(t *testing.T) {
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
		})
	}
}
