// Command migrate applies or reverts database migrations.
//
// Usage: migrate up | migrate down
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/db"
	"github.com/Yagna123k/causal-replay-lab/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 || (args[0] != "up" && args[0] != "down") {
		return fmt.Errorf("usage: migrate up|down")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, err := db.Connect(ctx, url, 2)
	if err != nil {
		return err
	}
	defer pool.Close()

	switch args[0] {
	case "up":
		done, err := db.MigrateUp(ctx, pool, migrations.FS)
		if err != nil {
			return err
		}
		if len(done) == 0 {
			fmt.Println("no pending migrations")
		}
		for _, v := range done {
			fmt.Printf("applied migration %04d\n", v)
		}
	case "down":
		v, err := db.MigrateDown(ctx, pool, migrations.FS)
		if err != nil {
			return err
		}
		if v == 0 {
			fmt.Println("nothing to revert")
		} else {
			fmt.Printf("reverted migration %04d\n", v)
		}
	}
	return nil
}
