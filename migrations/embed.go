// Package migrations embeds the SQL migration files so the migrate command
// and integration tests can apply them without external files.
package migrations

import "embed"

// FS holds all *.up.sql and *.down.sql files.
//
//go:embed *.sql
var FS embed.FS
