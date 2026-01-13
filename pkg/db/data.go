package db

import (
	"context"
	"database/sql"
)

// ReadData reads file data at the given offset
func (s *Store) ReadData(ctx context.Context, ino uint64, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, nil
	}

	chunkSize := s.chunkSize
	startChunk := offset / chunkSize
	endChunk := (offset + length - 1) / chunkSize

	rows, err := s.db.QueryContext(ctx,
		`SELECT chunk_index, data FROM fs_data
		 WHERE ino = ? AND chunk_index >= ? AND chunk_index <= ?
		 ORDER BY chunk_index`,
		ino, startChunk, endChunk)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]byte, 0, length)
	bytesRead := int64(0)

	for rows.Next() && bytesRead < length {
		var chunkIdx int64
		var data []byte
		if err := rows.Scan(&chunkIdx, &data); err != nil {
			return nil, err
		}

		chunkStart := chunkIdx * chunkSize

		// Calculate the portion of this chunk we need
		readStart := int64(0)
		if offset > chunkStart {
			readStart = offset - chunkStart
		}

		readEnd := int64(len(data))
		remaining := length - bytesRead
		if readEnd-readStart > remaining {
			readEnd = readStart + remaining
		}

		if readStart < int64(len(data)) && readEnd > readStart {
			if readEnd > int64(len(data)) {
				readEnd = int64(len(data))
			}
			result = append(result, data[readStart:readEnd]...)
			bytesRead += readEnd - readStart
		}
	}

	return result, rows.Err()
}

// WriteData writes data at the given offset
func (s *Store) WriteData(ctx context.Context, ino uint64, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	return s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.writeDataTx(ctx, tx, ino, offset, data)
	})
}

// WriteDataTx writes data within a transaction
func (s *Store) WriteDataTx(ctx context.Context, tx *sql.Tx, ino uint64, offset int64, data []byte) error {
	return s.writeDataTx(ctx, tx, ino, offset, data)
}

func (s *Store) writeDataTx(ctx context.Context, tx *sql.Tx, ino uint64, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	chunkSize := s.chunkSize
	dataLen := int64(len(data))
	dataOffset := int64(0)

	startChunk := offset / chunkSize
	endChunk := (offset + dataLen - 1) / chunkSize

	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		chunkStart := chunkIdx * chunkSize

		// Calculate write position within chunk
		writeStart := int64(0)
		if offset > chunkStart {
			writeStart = offset - chunkStart
		}

		// Calculate how much data to write to this chunk
		writeLen := chunkSize - writeStart
		if dataOffset+writeLen > dataLen {
			writeLen = dataLen - dataOffset
		}

		// Read existing chunk if we're doing a partial write
		var existingData []byte
		if writeStart > 0 || writeLen < chunkSize {
			row := tx.QueryRowContext(ctx,
				`SELECT data FROM fs_data WHERE ino = ? AND chunk_index = ?`,
				ino, chunkIdx)
			row.Scan(&existingData)
		}

		// Build new chunk data
		var newChunk []byte
		if len(existingData) > 0 {
			// Extend existing data if needed
			neededLen := writeStart + writeLen
			if int64(len(existingData)) < neededLen {
				newChunk = make([]byte, neededLen)
				copy(newChunk, existingData)
			} else {
				newChunk = make([]byte, len(existingData))
				copy(newChunk, existingData)
			}
		} else {
			// Create new chunk with zeros before write position if needed
			newChunk = make([]byte, writeStart+writeLen)
		}

		// Copy new data into chunk
		copy(newChunk[writeStart:], data[dataOffset:dataOffset+writeLen])

		// Store the chunk
		_, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO fs_data (ino, chunk_index, data) VALUES (?, ?, ?)`,
			ino, chunkIdx, newChunk)
		if err != nil {
			return err
		}

		dataOffset += writeLen
	}

	return nil
}

// Truncate truncates or extends a file to the specified size
func (s *Store) Truncate(ctx context.Context, ino uint64, size uint64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.truncateTx(ctx, tx, ino, size)
	})
}

// TruncateTx truncates within a transaction
func (s *Store) TruncateTx(ctx context.Context, tx *sql.Tx, ino uint64, size uint64) error {
	return s.truncateTx(ctx, tx, ino, size)
}

func (s *Store) truncateTx(ctx context.Context, tx *sql.Tx, ino uint64, size uint64) error {
	chunkSize := s.chunkSize

	if size == 0 {
		// Delete all chunks
		_, err := tx.ExecContext(ctx, `DELETE FROM fs_data WHERE ino = ?`, ino)
		return err
	}

	// Calculate the last chunk index we need
	lastChunk := (int64(size) - 1) / chunkSize

	// Delete chunks beyond the new size
	_, err := tx.ExecContext(ctx,
		`DELETE FROM fs_data WHERE ino = ? AND chunk_index > ?`,
		ino, lastChunk)
	if err != nil {
		return err
	}

	// Truncate the last chunk if needed
	offsetInLastChunk := int64(size) - lastChunk*chunkSize

	var existingData []byte
	err = tx.QueryRowContext(ctx,
		`SELECT data FROM fs_data WHERE ino = ? AND chunk_index = ?`,
		ino, lastChunk).Scan(&existingData)

	if err == sql.ErrNoRows {
		// No data at this chunk yet, nothing to truncate
		return nil
	}
	if err != nil {
		return err
	}

	if int64(len(existingData)) > offsetInLastChunk {
		// Truncate the chunk
		_, err = tx.ExecContext(ctx,
			`UPDATE fs_data SET data = ? WHERE ino = ? AND chunk_index = ?`,
			existingData[:offsetInLastChunk], ino, lastChunk)
		return err
	}

	return nil
}

// DeleteData deletes all data chunks for an inode
func (s *Store) DeleteData(ctx context.Context, ino uint64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fs_data WHERE ino = ?`, ino)
	return err
}

// DeleteDataTx deletes all data chunks within a transaction
func (s *Store) DeleteDataTx(ctx context.Context, tx *sql.Tx, ino uint64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM fs_data WHERE ino = ?`, ino)
	return err
}
