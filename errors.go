package main

import "fmt"

type codedError struct {
	Code string
	Msg  string
}

func (e *codedError) Error() string { return e.Msg }

func codedErrorf(code, format string, a ...any) error {
	return &codedError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

func codeOf(err error) string {
	if e, ok := err.(*codedError); ok {
		return e.Code
	}
	return ""
}
