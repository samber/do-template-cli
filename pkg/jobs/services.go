package jobs

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// DataRow represents a single row of data with dynamic fields
// This demonstrates flexible data handling in the do dependency injection system
type DataRow struct {
	Fields map[string]string `json:"fields"`
}

// DataProcessor defines the interface for data processing operations
// This interface demonstrates how to create extensible services with dependency injection
type DataProcessor interface {
	ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error)
	GetName() string
	GetDescription() string
}

// ProcessingResult represents the result of a data processing operation
type ProcessingResult struct {
	Success    bool     `json:"success"`
	Processed  int      `json:"processed"`
	OutputPath string   `json:"output_path,omitempty"`
	Errors     []string `json:"errors,omitempty"`
	Warnings   []string `json:"warnings,omitempty"`
	Processor  string   `json:"processor"`
}

// FileService handles file I/O operations
// This service demonstrates how to create reusable components with dependency injection
type FileService struct {
	logger zerolog.Logger `do:""`
}

// NewFileService creates a new file service with dependency injection
func NewFileService(i do.Injector) (*FileService, error) {
	return &FileService{
		logger: do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// ReadCSV reads a CSV file and returns data rows
// This method demonstrates file operations with proper error handling and logging
func (fs *FileService) ReadCSV(filepath string) ([]DataRow, error) {
	fs.logger.Info().Str("filepath", filepath).Msg("Reading CSV file")

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return []DataRow{}, nil
	}

	// Get headers from first row
	headers := records[0]
	var dataRows []DataRow

	for i, record := range records[1:] {
		if len(record) != len(headers) {
			fs.logger.Warn().Int("row", i+2).Msg("Row column count mismatch")
			continue
		}

		row := DataRow{Fields: make(map[string]string)}
		for j, value := range record {
			row.Fields[headers[j]] = value
		}
		dataRows = append(dataRows, row)
	}

	fs.logger.Info().Int("records", len(dataRows)).Msg("Successfully read CSV file")
	return dataRows, nil
}

// WriteJSON writes data rows to a JSON file
// This method demonstrates JSON serialization with proper error handling
func (fs *FileService) WriteJSON(filepath string, data interface{}) error {
	fs.logger.Info().Str("filepath", filepath).Msg("Writing JSON file")

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	fs.logger.Info().Str("filepath", filepath).Msg("Successfully wrote JSON file")
	return nil
}

// WriteCSV writes data rows to a CSV file
// This method demonstrates CSV writing with headers
func (fs *FileService) WriteCSV(filepath string, headers []string, data [][]string) error {
	fs.logger.Info().Str("filepath", filepath).Msg("Writing CSV file")

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	// Write data
	for _, record := range data {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	fs.logger.Info().Str("filepath", filepath).Msg("Successfully wrote CSV file")
	return nil
}

// GetFileStats returns basic statistics about a file
// This demonstrates file metadata operations
func (fs *FileService) GetFileStats(filepath string) (map[string]interface{}, error) {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	stats := map[string]interface{}{
		"size":        fileInfo.Size(),
		"permissions": fileInfo.Mode(),
		"modified":    fileInfo.ModTime(),
		"is_dir":      fileInfo.IsDir(),
	}

	return stats, nil
}
