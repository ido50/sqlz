package sqlz


type Statment struct {
	ErrHandlers []func(err error)
}

func (stmt *Statment) HandlerError(err error) {
	if stmt.ErrHandlers != nil {
		for _, handler := range stmt.ErrHandlers {
			handler(err)
		}
	}
}