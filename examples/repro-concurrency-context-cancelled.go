package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-errors/errors"
	"github.com/marcboeker/go-duckdb"
	"golang.org/x/exp/slog"
)

type (
	Result struct {
		Columns     []string          `json:"columns,omitempty"`
		ColumnTypes []*sql.ColumnType `json:"column_types,omitempty"`
		Rows        []interface{}     `json:"rows,omitempty"`
		NextOffset  int               `json:"next_offset,omitempty"`
	}
)

type newdb struct {
	db  *sql.DB
	cfg DuckConfig
}

func (ndb *newdb) Close() error { return ndb.db.Close() }

type DuckConfig struct {
	Filename        string
	ParallelQueries bool
}

func NewDuckDB(cfg *DuckConfig) (*newdb, error) {
	ctx := context.Background()
	ctxConn, cancelConn := context.WithTimeout(ctx, 1*time.Second)
	defer cancelConn()
	absolutePath, err := filepath.Abs(cfg.Filename)
	if err != nil {
		return nil, err
	}
	connStr := fmt.Sprintf("%v?access_mode=READ_WRITE", absolutePath)
	connector, err := duckdb.NewConnector(connStr, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'httpfs'",
			"LOAD 'httpfs'",
		}
		for _, qry := range bootQueries {
			_, err := execer.ExecContext(ctxConn, qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	err = db.PingContext(ctxConn)
	if err != nil {
		return nil, err
	}

	setting := db.QueryRow("SELECT current_setting('access_mode')")
	var am string
	err = setting.Scan(&am)
	if err != nil {
		return nil, err
	}
	slog.Info("NewDB",
		"access_mode", am,
		"cfg", fmt.Sprintf("%+v", *cfg),
	)

	ndb := &newdb{
		db:  db,
		cfg: *cfg,
	}
	return ndb, err
}

func ConfigForTests() *DuckConfig {
	cfg := &DuckConfig{
		Filename: fmt.Sprintf("duckdb%v.db", rand.Int()),
	}
	return cfg
}

func (ndb *newdb) Query(ctx context.Context, sql string, res *Result) error {
	rows, err := ndb.db.QueryContext(ctx, sql)
	if err != nil {
		err = errors.Errorf("failed on Query(%v), err:%w", sql, err)
		return err
	}
	defer rows.Close()
	res.Columns, err = rows.Columns()
	if err != nil {
		return err
	}
	res.ColumnTypes, err = rows.ColumnTypes()
	if err != nil {
		return err
	}
	for rows.Next() {
		rowPointers := make([]interface{}, len(res.ColumnTypes))
		for i := range rowPointers {
			rowPointers[i] = new(interface{})
		}
		err = rows.Scan(rowPointers...)
		if err != nil {
			return err
		}
		res.Rows = append(res.Rows, rowPointers)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return err
}

func reproBug(b *testing.B, ndb *newdb) {
	slog.Info("starting test..")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx := context.TODO()
			res := &Result{
				Rows: make([]interface{}, 0),
			}
			var err error
			stmt := `SELECT * FROM lineitem LIMIT 100 offset 100;`
			err = ndb.Query(ctx, stmt, res)
			if err != nil {
				bugRepro := strings.Contains(err.Error(), "context canceled")
				slog.Error("REPRODUCED BUG?", "error", err, "bugRepro", bugRepro)
				if bugRepro {
					b.FailNow()
				} else {
					b.Errorf("test failed w/ unexpected error: %w", err)
				}
			}
		}
	})
	b.StopTimer()
	slog.Info("testing finished")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rps")
}

func setupDuckDB() *newdb {
	slog.Info("connecting to DB")
	ndb, err := NewDuckDB(ConfigForTests())
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("creating tables")
	_, err = ndb.db.Exec(`CREATE TABLE IF NOT EXISTS lineitem AS SELECT * FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet' LIMIT 10000;`)
	if err != nil {
		log.Fatal(err)
	}
	return ndb
}

func main() {
	ndb := setupDuckDB()
	defer ndb.Close()
	time.Sleep(400 * time.Millisecond)
	testing.Benchmark(func(b *testing.B) {
		reproBug(b, ndb)
	})

}
