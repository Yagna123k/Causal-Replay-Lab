package db

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is the advisory lock key that serialises concurrent migrators.
const migrationLockID int64 = 727_001

var migrationFile = regexp.MustCompile(`^(\d+)_.+\.(up|down)\.sql$`)

type migration struct {
	version int64
	name    string
	up      string
	down    string
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	byVersion := map[int64]*migration{}
	for _, e := range entries {
		m := migrationFile.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		v, _ := strconv.ParseInt(m[1], 10, 64)
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		mg := byVersion[v]
		if mg == nil {
			mg = &migration{version: v, name: e.Name()}
			byVersion[v] = mg
		}
		if m[2] == "up" {
			mg.up = string(body)
		} else {
			mg.down = string(body)
		}
	}
	out := make([]migration, 0, len(byVersion))
	for _, mg := range byVersion {
		if mg.up == "" {
			return nil, fmt.Errorf("migration %d has no up file", mg.version)
		}
		out = append(out, *mg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// withLock runs fn on a dedicated connection holding the migration advisory lock.
func withLock(ctx context.Context, pool *pgxpool.Pool, fn func(conn *pgxpool.Conn) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return fn(conn)
}

func applied(ctx context.Context, conn *pgxpool.Conn) (map[int64]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func runInTx(ctx context.Context, conn *pgxpool.Conn, sql, bookkeeping string, version int64) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, bookkeeping, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MigrateUp applies all pending migrations in order and returns the versions applied.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]int64, error) {
	migs, err := loadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	var done []int64
	err = withLock(ctx, pool, func(conn *pgxpool.Conn) error {
		have, err := applied(ctx, conn)
		if err != nil {
			return err
		}
		for _, m := range migs {
			if have[m.version] {
				continue
			}
			if err := runInTx(ctx, conn, m.up, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.version); err != nil {
				return fmt.Errorf("apply %s: %w", m.name, err)
			}
			done = append(done, m.version)
		}
		return nil
	})
	return done, err
}

// MigrateDown reverts the most recently applied migration. It returns 0 if none was applied.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) (int64, error) {
	migs, err := loadMigrations(fsys)
	if err != nil {
		return 0, err
	}
	var reverted int64
	err = withLock(ctx, pool, func(conn *pgxpool.Conn) error {
		var latest int64
		err := conn.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&latest)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
		if latest == 0 {
			return nil
		}
		for _, m := range migs {
			if m.version != latest {
				continue
			}
			if m.down == "" {
				return fmt.Errorf("migration %d has no down file", latest)
			}
			if err := runInTx(ctx, conn, m.down, `DELETE FROM schema_migrations WHERE version = $1`, latest); err != nil {
				return fmt.Errorf("revert %s: %w", m.name, err)
			}
			reverted = latest
			return nil
		}
		return fmt.Errorf("applied migration %d not found in migration files", latest)
	})
	return reverted, err
}
