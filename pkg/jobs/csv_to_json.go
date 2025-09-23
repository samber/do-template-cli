package jobs

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// CSVToJSONService handles CSV to JSON conversion operations
// This service demonstrates data format transformation with dependency injection.
type CSVToJSONService struct {
	fileService *FileService   `do:""`
	logger      zerolog.Logger `do:""`
}

// NewCSVToJSONService creates a new CSV to JSON service with dependency injection.
func NewCSVToJSONService(i do.Injector) (*CSVToJSONService, error) {
	return &CSVToJSONService{
		fileService: do.MustInvoke[*FileService](i),
		logger:      do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// ProcessData converts CSV data to JSON format
// This method demonstrates the DataProcessor interface implementation.
func (s *CSVToJSONService) ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error) {
	s.logger.Info().Msg("Converting CSV data to JSON format")

	// For CSV to JSON conversion, we typically work with file paths
	inputFile, ok := options["input_file"].(string)
	if !ok {
		return nil, errors.New("input_file option is required")
	}

	// Read the CSV file
	dataRows, err := s.fileService.ReadCSV(inputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %w", err)
	}

	// Generate output file path if not provided
	outputFile, _ := options["output_file"].(string)
	if outputFile == "" {
		ext := filepath.Ext(inputFile)
		outputFile = strings.TrimSuffix(inputFile, ext) + ".json"
	}

	// Write to JSON file
	if err := s.fileService.WriteJSON(outputFile, dataRows); err != nil {
		return nil, fmt.Errorf("failed to write JSON file: %w", err)
	}

	s.logger.Info().
		Str("input", inputFile).
		Str("output", outputFile).
		Int("records", len(dataRows)).
		Msg("Successfully converted CSV to JSON")

	return dataRows, nil
}

// GetName returns the processor name.
func (s *CSVToJSONService) GetName() string {
	return "csv-to-json"
}

// GetDescription returns the processor description.
func (s *CSVToJSONService) GetDescription() string {
	return "Convert CSV files to JSON format"
}

// ConvertFile converts a single CSV file to JSON
// This convenience method demonstrates file-level operations.
func (s *CSVToJSONService) ConvertFile(inputPath, outputPath string) (*ProcessingResult, error) {
	s.logger.Info().
		Str("input", inputPath).
		Str("output", outputPath).
		Msg("Starting CSV to JSON conversion")

	options := map[string]interface{}{
		"input_file":  inputPath,
		"output_file": outputPath,
	}

	dataRows, err := s.ProcessData(nil, options)
	if err != nil {
		return &ProcessingResult{
			Success:   false,
			Processed: 0,
			Processor: s.GetName(),
			Errors:    []string{err.Error()},
		}, err
	}

	return &ProcessingResult{
		Success:    true,
		Processed:  len(dataRows),
		OutputPath: outputPath,
		Processor:  s.GetName(),
	}, nil
}

// BatchConvert converts multiple CSV files to JSON
// This method demonstrates batch processing capabilities.
func (s *CSVToJSONService) BatchConvert(inputPaths []string, outputDir string) ([]*ProcessingResult, error) {
	s.logger.Info().
		Int("file_count", len(inputPaths)).
		Str("output_dir", outputDir).
		Msg("Starting batch CSV to JSON conversion")

	results := []*ProcessingResult{}

	for _, inputPath := range inputPaths {
		filename := filepath.Base(inputPath)
		ext := filepath.Ext(filename)
		outputFilename := strings.TrimSuffix(filename, ext) + ".json"
		outputPath := filepath.Join(outputDir, outputFilename)

		result, err := s.ConvertFile(inputPath, outputPath)
		if err != nil {
			s.logger.Error().Err(err).Str("file", inputPath).Msg("Failed to convert file")
		}
		results = append(results, result)
	}

	s.logger.Info().
		Int("successful", len(results)).
		Msg("Batch conversion completed")

	return results, nil
}
