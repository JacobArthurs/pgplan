package plan

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Execute(dbConn string, sql string) ([]ExplainOutput, error) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dbConn)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := "EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) " + sql

	var jsonStr string
	err = tx.QueryRow(ctx, query).Scan(&jsonStr)
	if err != nil {
		return nil, fmt.Errorf("executing EXPLAIN: %w", err)
	}

	return ParseJSONPlan([]byte(jsonStr))
}
