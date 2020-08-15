package sqlz

import (
	"strings"
)

// JSONBObject represents a PostgreSQL JSONB object.
type JSONBObject struct {
	// Bindings is the list of bindings for the object.
	Bindings []interface{}
}

// JSONBBuilder represents usage of PostgreSQL's jsonb_build_array or
// jsonb_build_object functions.
type JSONBBuilder struct {
	// Array indicates whether an array is being built, or an object
	Array bool
	// Bindings is the list of bindings for the function call
	Bindings []interface{}
}

// BuildJSONBObject creates a call to jsonb_build_object.
func BuildJSONBObject(in map[string]interface{}) (out JSONBBuilder) {
	for _, key := range sortKeys(in) {
		val := in[key]
		out.Bindings = append(out.Bindings, key, val)
	}

	return out
}

// BuildJSONBArray creates a call to jsonb_build_array.
func BuildJSONBArray(in ...interface{}) (out JSONBBuilder) {
	out.Array = true
	out.Bindings = append(out.Bindings, in...)

	return out
}

// Parse processes the JSONB object creator, returning SQL code that calls
// the appropriate function.
func (b JSONBBuilder) Parse() (asSQL string, bindings []interface{}) {
	asSQL = "jsonb_build_"
	if b.Array {
		asSQL += "array("
	} else {
		asSQL += "object("
	}

	var placeholders []string

	for _, val := range b.Bindings {
		if object, isObject := val.(map[string]interface{}); isObject {
			subSQL, subBindings := BuildJSONBObject(object).Parse()
			placeholders = append(placeholders, subSQL)
			bindings = append(bindings, subBindings...)
		} else if array, isArray := val.([]interface{}); isArray {
			subSQL, subBindings := BuildJSONBArray(array...).Parse()
			placeholders = append(placeholders, subSQL)
			bindings = append(bindings, subBindings...)
		} else {
			placeholders = append(placeholders, "?")
			bindings = append(bindings, val)
		}
	}

	return asSQL + strings.Join(placeholders, ", ") + ")", bindings
}
