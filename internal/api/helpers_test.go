package api

import (
	"io"
	"log/slog"
)

func newTestHandlerLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
