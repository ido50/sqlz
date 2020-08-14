package sqlz

type Statement struct {
	ErrHandlers []func(err error)
}

func (stmt *Statement) HandlerError(err error) {
	if stmt.ErrHandlers != nil {
		for _, handler := range stmt.ErrHandlers {
			handler(err)
		}
	}
}
