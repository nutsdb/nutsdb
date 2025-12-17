package nutsdb

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
)

func TestHintFileBasicOperations(t *testing.T) {
	// Test HintEntry encoding and decoding
	entry := &HintEntry{
		BucketId:  1,
		KeySize:   3,
		ValueSize: 5,
		Timestamp: 1234567890,
		TTL:       3600,
		Flag:      core.DataSetFlag,
		Status:    core.Committed,
		Ds:        core.DataStructureBTree,
		DataPos:   100,
		FileID:    1,
		Key:       []byte("key"),
	}

	// Test encoding
	encoded := entry.Encode()
	if len(encoded) == 0 {
		t.Fatal("Failed to encode hint entry")
	}

	// Test decoding
	decoded := &HintEntry{}
	err := decoded.Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode hint entry: %v", err)
	}

	// Verify decoded values
	if decoded.BucketId != entry.BucketId {
		t.Errorf("Expected BucketId %d, got %d", entry.BucketId, decoded.BucketId)
	}
	if decoded.KeySize != entry.KeySize {
		t.Errorf("Expected KeySize %d, got %d", entry.KeySize, decoded.KeySize)
	}
	if decoded.ValueSize != entry.ValueSize {
		t.Errorf("Expected ValueSize %d, got %d", entry.ValueSize, decoded.ValueSize)
	}
	if decoded.Timestamp != entry.Timestamp {
		t.Errorf("Expected Timestamp %d, got %d", entry.Timestamp, decoded.Timestamp)
	}
	if decoded.TTL != entry.TTL {
		t.Errorf("Expected TTL %d, got %d", entry.TTL, decoded.TTL)
	}
	if decoded.Flag != entry.Flag {
		t.Errorf("Expected Flag %d, got %d", entry.Flag, decoded.Flag)
	}
	if decoded.Status != entry.Status {
		t.Errorf("Expected Status %d, got %d", entry.Status, decoded.Status)
	}
	if decoded.Ds != entry.Ds {
		t.Errorf("Expected Ds %d, got %d", entry.Ds, decoded.Ds)
	}
	if decoded.DataPos != entry.DataPos {
		t.Errorf("Expected DataPos %d, got %d", entry.DataPos, decoded.DataPos)
	}
	if decoded.FileID != entry.FileID {
		t.Errorf("Expected FileID %d, got %d", entry.FileID, decoded.FileID)
	}
	if string(decoded.Key) != string(entry.Key) {
		t.Errorf("Expected Key %s, got %s", string(entry.Key), string(decoded.Key))
	}
}

func TestHintEntrySize(t *testing.T) {
	entry := &HintEntry{
		BucketId:  1,
		KeySize:   3,
		ValueSize: 5,
		Timestamp: 1234567890,
		TTL:       3600,
		Flag:      core.DataSetFlag,
		Status:    core.Committed,
		Ds:        core.DataStructureBTree,
		DataPos:   100,
		FileID:    1,
		Key:       []byte("key"),
	}

	expectedSize := entry.Size()
	encoded := entry.Encode()
	actualSize := int64(len(encoded))

	if expectedSize != actualSize {
		t.Errorf("Expected size %d, got %d", expectedSize, actualSize)
	}
}

func TestHintEntryEncodeDecodeEdgeCases(t *testing.T) {
	testCases := []struct {
		name  string
		entry *HintEntry
	}{
		{
			name: "Empty key",
			entry: &HintEntry{
				BucketId:  1,
				KeySize:   0,
				ValueSize: 5,
				Timestamp: 1234567890,
				TTL:       3600,
				Flag:      core.DataSetFlag,
				Status:    core.Committed,
				Ds:        core.DataStructureBTree,
				DataPos:   100,
				FileID:    1,
				Key:       []byte(""),
			},
		},
		{
			name: "Large key",
			entry: &HintEntry{
				BucketId:  1,
				KeySize:   1000,
				ValueSize: 5,
				Timestamp: 1234567890,
				TTL:       3600,
				Flag:      core.DataSetFlag,
				Status:    core.Committed,
				Ds:        core.DataStructureBTree,
				DataPos:   100,
				FileID:    1,
				Key:       make([]byte, 1000),
			},
		},
		{
			name: "Zero TTL",
			entry: &HintEntry{
				BucketId:  1,
				KeySize:   3,
				ValueSize: 5,
				Timestamp: 1234567890,
				TTL:       0,
				Flag:      core.DataSetFlag,
				Status:    core.Committed,
				Ds:        core.DataStructureBTree,
				DataPos:   100,
				FileID:    1,
				Key:       []byte("key"),
			},
		},
		{
			name: "Different data structures",
			entry: &HintEntry{
				BucketId:  1,
				KeySize:   3,
				ValueSize: 5,
				Timestamp: 1234567890,
				TTL:       3600,
				Flag:      core.DataLPushFlag,
				Status:    core.Committed,
				Ds:        core.DataStructureList,
				DataPos:   100,
				FileID:    1,
				Key:       []byte("key"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := tc.entry.Encode()
			if len(encoded) == 0 {
				t.Fatal("Failed to encode hint entry")
			}

			decoded := &HintEntry{}
			err := decoded.Decode(encoded)
			if err != nil {
				t.Fatalf("Failed to decode hint entry: %v", err)
			}

			// Verify key values match
			if decoded.BucketId != tc.entry.BucketId {
				t.Errorf("Expected BucketId %d, got %d", tc.entry.BucketId, decoded.BucketId)
			}
			if decoded.KeySize != tc.entry.KeySize {
				t.Errorf("Expected KeySize %d, got %d", tc.entry.KeySize, decoded.KeySize)
			}
			if !stringEqual(decoded.Key, tc.entry.Key) {
				t.Errorf("Expected Key %s, got %s", string(tc.entry.Key), string(decoded.Key))
			}
		})
	}
}

func stringEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHintFileWriterReader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hintfile-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	hintPath := filepath.Join(tmpDir, "1.hint")

	// Test writing
	writer := &HintFileWriter{}
	err = writer.Create(hintPath)
	if err != nil {
		t.Fatalf("Failed to create hint file: %v", err)
	}

	// Write some entries
	entries := []*HintEntry{
		{
			BucketId:  1,
			KeySize:   uint32(len([]byte("key1"))),
			ValueSize: 5,
			Timestamp: 1234567890,
			TTL:       3600,
			Flag:      core.DataSetFlag,
			Status:    core.Committed,
			Ds:        core.DataStructureBTree,
			DataPos:   100,
			FileID:    1,
			Key:       []byte("key1"),
		},
		{
			BucketId:  2,
			KeySize:   uint32(len([]byte("key2"))),
			ValueSize: 7,
			Timestamp: 1234567891,
			TTL:       7200,
			Flag:      core.DataSetFlag,
			Status:    core.Committed,
			Ds:        core.DataStructureSet,
			DataPos:   200,
			FileID:    1,
			Key:       []byte("key2"),
		},
	}

	for _, entry := range entries {
		err = writer.Write(entry)
		if err != nil {
			t.Fatalf("Failed to write hint entry: %v", err)
		}
	}

	err = writer.Sync()
	if err != nil {
		t.Fatalf("Failed to sync hint file: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close hint file: %v", err)
	}

	// Test reading
	reader := &HintFileReader{}
	err = reader.Open(hintPath)
	if err != nil {
		t.Fatalf("Failed to open hint file: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Failed to close hint file reader: %v", err)
		}
	}()

	// Read and verify entries
	for i, expectedEntry := range entries {
		entry, err := reader.Read()
		if err != nil {
			t.Fatalf("Failed to read hint entry %d: %v", i, err)
		}

		if entry.BucketId != expectedEntry.BucketId {
			t.Errorf("Entry %d: Expected BucketId %d, got %d", i, expectedEntry.BucketId, entry.BucketId)
		}
		if string(entry.Key) != string(expectedEntry.Key) {
			t.Errorf("Entry %d: Expected Key %s, got %s", i, string(expectedEntry.Key), string(entry.Key))
		}
		if entry.DataPos != expectedEntry.DataPos {
			t.Errorf("Entry %d: Expected DataPos %d, got %d", i, expectedEntry.DataPos, entry.DataPos)
		}

		t.Logf("Successfully read entry %d: BucketId=%d, Key=%s, DataPos=%d",
			i, entry.BucketId, string(entry.Key), entry.DataPos)
	}

	// Should get EOF at the end
	_, err = reader.Read()
	if err != nil {
		if err != io.EOF && err != ErrIndexOutOfBound && err != ErrEntryZero && err != core.ErrHeaderSizeOutOfBounds {
			t.Fatalf("Expected EOF or similar error, got: %v", err)
		}
	}
}

func TestHintFileWriterErrorHandling(t *testing.T) {
	t.Run("Write nil entry", func(t *testing.T) {
		writer := &HintFileWriter{}
		err := writer.Write(nil)
		if err != ErrHintFileEntryInvalid {
			t.Errorf("Expected ErrHintFileEntryInvalid, got %v", err)
		}
	})

	t.Run("Create file in non-existent directory", func(t *testing.T) {
		writer := &HintFileWriter{}
		err := writer.Create("/non/existent/path/test.hint")
		if err == nil {
			t.Error("Expected error when creating file in non-existent directory")
		}
	})

	t.Run("Sync without file", func(t *testing.T) {
		writer := &HintFileWriter{}
		err := writer.Sync()
		if err != nil {
			t.Errorf("Unexpected error when syncing without file: %v", err)
		}
	})

	t.Run("Close without file", func(t *testing.T) {
		writer := &HintFileWriter{}
		err := writer.Close()
		if err != nil {
			t.Errorf("Unexpected error when closing without file: %v", err)
		}
	})
}

func TestHintFileReaderErrorHandling(t *testing.T) {
	t.Run("Open non-existent file", func(t *testing.T) {
		reader := &HintFileReader{}
		err := reader.Open("/non/existent/path/test.hint")
		if err == nil {
			t.Error("Expected error when opening non-existent file")
		}
	})

	t.Run("Read without opening file", func(t *testing.T) {
		reader := &HintFileReader{}
		_, err := reader.Read()
		if err != ErrHintFileEntryInvalid {
			t.Errorf("Expected ErrHintFileEntryInvalid, got %v", err)
		}
	})

	t.Run("Read corrupted file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "hintfile-test")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		corruptedPath := filepath.Join(tmpDir, "corrupted.hint")
		// Create a corrupted file with invalid data
		err = os.WriteFile(corruptedPath, []byte{0xFF, 0xFF, 0xFF}, 0644)
		if err != nil {
			t.Fatalf("Failed to create corrupted file: %v", err)
		}

		reader := &HintFileReader{}
		err = reader.Open(corruptedPath)
		if err != nil {
			t.Fatalf("Failed to open corrupted file: %v", err)
		}
		defer func() {
			if err := reader.Close(); err != nil {
				t.Errorf("Failed to close corrupted file reader: %v", err)
			}
		}()

		_, err = reader.Read()
		if err == nil {
			t.Error("Expected error when reading corrupted file")
		}
	})

	t.Run("Read empty file", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "hintfile-test")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		emptyPath := filepath.Join(tmpDir, "empty.hint")
		// Create an empty file
		err = os.WriteFile(emptyPath, []byte{}, 0644)
		if err != nil {
			t.Fatalf("Failed to create empty file: %v", err)
		}

		reader := &HintFileReader{}
		err = reader.Open(emptyPath)
		if err != nil {
			t.Fatalf("Failed to open empty file: %v", err)
		}
		defer reader.Close()

		_, err = reader.Read()
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
	})
}

func TestHintEntryDecodeInvalidData(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expectedErr error
	}{
		{
			name:        "Empty data",
			data:        []byte{},
			expectedErr: ErrHintFileEntryInvalid,
		},
		{
			name:        "Incomplete header",
			data:        []byte{0x01}, // Only bucket ID, missing other fields
			expectedErr: ErrHintFileCorrupted,
		},
		{
			name:        "Invalid varint",
			data:        []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // Invalid varint
			expectedErr: ErrHintFileCorrupted,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entry := &HintEntry{}
			err := entry.Decode(tc.data)
			if err != tc.expectedErr {
				t.Errorf("Expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestHintFilePartialRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "hintfile-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	hintPath := filepath.Join(tmpDir, "partial.hint")

	// Create a hint file with multiple entries
	writer := &HintFileWriter{}
	err = writer.Create(hintPath)
	if err != nil {
		t.Fatalf("Failed to create hint file: %v", err)
	}

	entries := []*HintEntry{
		{
			BucketId:  1,
			KeySize:   3,
			ValueSize: 5,
			Timestamp: 1234567890,
			TTL:       3600,
			Flag:      core.DataSetFlag,
			Status:    core.Committed,
			Ds:        core.DataStructureBTree,
			DataPos:   100,
			FileID:    1,
			Key:       []byte("key1"),
		},
		{
			BucketId:  2,
			KeySize:   3,
			ValueSize: 5,
			Timestamp: 1234567891,
			TTL:       3600,
			Flag:      core.DataSetFlag,
			Status:    core.Committed,
			Ds:        core.DataStructureBTree,
			DataPos:   200,
			FileID:    1,
			Key:       []byte("key2"),
		},
	}

	for _, entry := range entries {
		err = writer.Write(entry)
		if err != nil {
			t.Fatalf("Failed to write hint entry: %v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close hint file: %v", err)
	}

	// Read only the first entry
	reader := &HintFileReader{}
	err = reader.Open(hintPath)
	if err != nil {
		t.Fatalf("Failed to open hint file: %v", err)
	}

	// Read first entry
	entry, err := reader.Read()
	if err != nil {
		t.Fatalf("Failed to read first hint entry: %v", err)
	}

	if string(entry.Key) != "key1" {
		t.Errorf("Expected key1, got %s", string(entry.Key))
	}

	// Close reader before reading the second entry
	err = reader.Close()
	if err != nil {
		t.Fatalf("Failed to close hint file: %v", err)
	}

	// Reopen and read all entries
	reader = &HintFileReader{}
	err = reader.Open(hintPath)
	if err != nil {
		t.Fatalf("Failed to reopen hint file: %v", err)
	}
	defer reader.Close()

	// Read all entries
	readCount := 0
	for {
		_, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Failed to read hint entry: %v", err)
		}
		readCount++
	}

	if readCount != len(entries) {
		t.Errorf("Expected to read %d entries, got %d", len(entries), readCount)
	}
}

func TestGetHintPath(t *testing.T) {
	dir := "/tmp/nutsdb"
	fid := int64(123)
	expected := "/tmp/nutsdb/123.hint"
	actual := getHintPath(fid, dir)
	if actual != expected {
		t.Errorf("Expected %s, got %s", expected, actual)
	}
}
