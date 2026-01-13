package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Common errors
var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already exists")
	ErrNotEmpty = errors.New("directory not empty")
)

// Store provides all database operations for the filesystem
type Store struct {
	db        *sql.DB
	chunkSize int64
}

// Config holds database configuration
type Config struct {
	Path        string
	ChunkSize   int64
	BusyTimeout time.Duration
}

// DefaultConfig returns a config with sensible defaults
func DefaultConfig(path string) Config {
	return Config{
		Path:        path,
		ChunkSize:   4096,
		BusyTimeout: 5 * time.Second,
	}
}

// Open opens or creates a SQLite database for the filesystem
func Open(cfg Config) (*Store, error) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.BusyTimeout <= 0 {
		cfg.BusyTimeout = 5 * time.Second
	}

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=%d&_foreign_keys=on&_synchronous=NORMAL",
		cfg.Path,
		cfg.BusyTimeout.Milliseconds(),
	)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Use single connection to avoid locking issues
	db.SetMaxOpenConns(1)

	store := &Store{
		db:        db,
		chunkSize: cfg.ChunkSize,
	}

	// Initialize schema
	if err := store.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Read chunk size from config (may have been set previously)
	var chunkSizeStr string
	err = db.QueryRow("SELECT value FROM fs_config WHERE key = 'chunk_size'").Scan(&chunkSizeStr)
	if err == nil {
		fmt.Sscanf(chunkSizeStr, "%d", &store.chunkSize)
	}

	return store, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// ChunkSize returns the configured chunk size
func (s *Store) ChunkSize() int64 {
	return s.chunkSize
}

// WithTx executes a function within a transaction
func (s *Store) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// DB returns the underlying database connection (for direct queries)
func (s *Store) DB() *sql.DB {
	return s.db
}
