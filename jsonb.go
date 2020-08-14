package sqlz

import (
	"strings"
)

type JSONBObject struct {
	Bindings []interface{}
}

type JSONBBuilder struct {
	Array    bool
	Bindings []interface{}
}

func BuildJSONBObject(in map[string]interface{}) (out JSONBBuilder) {
	for _, key := range sortKeys(in) {
		val := in[key]
		out.Bindings = append(out.Bindings, key, val)
	}

	return out
}

func BuildJSONBArray(in ...interface{}) (out JSONBBuilder) {
	out.Array = true
	out.Bindings = append(out.Bindings, in...)

	return out
}

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
