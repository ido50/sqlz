package sqlz

import "testing"

func TestJSONBBuilder(t *testing.T) {
	runTests(t, func(dbz *DB) []test {
		return []test{
			test{
				"create a complex JSONB object",
				dbz.InsertInto("table").Columns("data").Values(
					BuildJSONBObject(map[string]interface{}{
						"string": "This is a string",
						"number": 3,
						"object": map[string]interface{}{
							"subfield": "subval",
							"subarray": []interface{}{1, 2, "3"},
						},
						"array": []interface{}{"one", "two", "three"},
					}),
				),
				"INSERT INTO table (data) VALUES (jsonb_build_object(?, ?, ?, ?, ?, jsonb_build_object(?, ?, ?, jsonb_build_array(?, ?, ?)), ?, jsonb_build_array(?, ?, ?)))",
				[]interface{}{"string", "This is a string", "number", 3, "object", "subfield", "subval", "subarray", 1, 2, "3", "array", "one", "two", "three"},
			},
		}
	})
}
