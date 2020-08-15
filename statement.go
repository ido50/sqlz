package sqlz

// Statement is a base struct for all statement types in the library.
type Statement struct {
	// ErrHandlers is a list of error handler functions
	ErrHandlers []func(err error)
}

// HandleError receives an error value, and executes all of the statements
// error handlers with it.
func (stmt *Statement) HandleError(err error) {
	if stmt.ErrHandlers != nil {
		for _, handler := range stmt.ErrHandlers {
			handler(err)
		}
	}
}
