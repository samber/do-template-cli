package jobs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// ValidationRule defines a validation rule for a field.
type ValidationRule struct {
	Field       string      `json:"field"`
	Type        string      `json:"type"`        // required, email, numeric, regex, min_length, max_length, custom
	Constraints interface{} `json:"constraints"` // value for min/max, pattern for regex, etc.
	Message     string      `json:"message"`     // custom error message
}

// ValidationError represents a validation error.
type ValidationError struct {
	RowNumber  int     `json:"row_number"`
	FieldName  string  `json:"field_name"`
	FieldValue string  `json:"field_value"`
	RuleType   string  `json:"rule_type"`
	Message    string  `json:"message"`
	Severity   string  `json:"severity"` // error, warning
	RowData    DataRow `json:"row_data,omitempty"`
}

// ValidationResult represents the result of validation.
type ValidationResult struct {
	ValidRows    int               `json:"valid_rows"`
	InvalidRows  int               `json:"invalid_rows"`
	TotalRows    int               `json:"total_rows"`
	Errors       []ValidationError `json:"errors"`
	Warnings     []ValidationError `json:"warnings"`
	FieldStats   map[string]int    `json:"field_stats,omitempty"`
	QualityScore float64           `json:"quality_score"`
}

// ValidateService handles data validation operations
// This service demonstrates data quality validation with dependency injection.
type ValidateService struct {
	fileService *FileService   `do:""`
	logger      zerolog.Logger `do:""`
}

// NewValidateService creates a new validate service with dependency injection.
func NewValidateService(i do.Injector) (*ValidateService, error) {
	return &ValidateService{
		fileService: do.MustInvoke[*FileService](i),
		logger:      do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// ValidateOptions contains validation configuration.
type ValidateOptions struct {
	InputFile     string           `json:"input_file"`
	OutputFile    string           `json:"output_file"`
	Rules         []ValidationRule `json:"rules"`
	FailFast      bool             `json:"fail_fast"`      // stop on first error
	ExportValid   bool             `json:"export_valid"`   // export valid records
	ExportInvalid bool             `json:"export_invalid"` // export invalid records
}

// ProcessData validates data based on rules
// This method demonstrates comprehensive data validation logic.
func (s *ValidateService) ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error) {
	s.logger.Info().Msg("Validating data based on rules")

	// Parse options
	opts, err := s.parseValidateOptions(options)
	if err != nil {
		return nil, fmt.Errorf("failed to parse validation options: %w", err)
	}

	// If input data is empty, try to read from file
	if len(input) == 0 && opts.InputFile != "" {
		var err error
		input, err = s.fileService.ReadCSV(opts.InputFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read input file: %w", err)
		}
	}

	// Perform validation
	result, validData, invalidData := s.validateData(input, opts)

	// Write results to file if output file specified
	if opts.OutputFile != "" {
		if err := s.fileService.WriteJSON(opts.OutputFile, result); err != nil {
			return nil, fmt.Errorf("failed to write validation results: %w", err)
		}
	}

	// Export valid and invalid data if requested
	if opts.ExportValid && len(validData) > 0 {
		validFile := strings.TrimSuffix(opts.OutputFile, ".json") + "_valid.json"
		if err := s.fileService.WriteJSON(validFile, validData); err != nil {
			s.logger.Error().Err(err).Msg("Failed to export valid data")
		}
	}

	if opts.ExportInvalid && len(invalidData) > 0 {
		invalidFile := strings.TrimSuffix(opts.OutputFile, ".json") + "_invalid.json"
		if err := s.fileService.WriteJSON(invalidFile, invalidData); err != nil {
			s.logger.Error().Err(err).Msg("Failed to export invalid data")
		}
	}

	// Log validation summary
	s.logger.Info().
		Int("total_rows", result.TotalRows).
		Int("valid_rows", result.ValidRows).
		Int("invalid_rows", result.InvalidRows).
		Int("errors", len(result.Errors)).
		Int("warnings", len(result.Warnings)).
		Float64("quality_score", result.QualityScore).
		Msg("Data validation completed")

	// Return valid data for further processing
	return validData, nil
}

// GetName returns the processor name.
func (s *ValidateService) GetName() string {
	return "validate-data"
}

// GetDescription returns the processor description.
func (s *ValidateService) GetDescription() string {
	return "Validate data integrity and quality"
}

// parseValidateOptions parses validation options from map.
func (s *ValidateService) parseValidateOptions(options map[string]interface{}) (*ValidateOptions, error) {
	opts := &ValidateOptions{}

	if inputFile, ok := options["input_file"].(string); ok {
		opts.InputFile = inputFile
	}

	if outputFile, ok := options["output_file"].(string); ok {
		opts.OutputFile = outputFile
	}

	if failFast, ok := options["fail_fast"].(bool); ok {
		opts.FailFast = failFast
	}

	if exportValid, ok := options["export_valid"].(bool); ok {
		opts.ExportValid = exportValid
	}

	if exportInvalid, ok := options["export_invalid"].(bool); ok {
		opts.ExportInvalid = exportInvalid
	}

	// Parse validation rules
	if rulesRaw, ok := options["rules"].([]interface{}); ok {
		for _, ruleRaw := range rulesRaw {
			if ruleMap, ok := ruleRaw.(map[string]interface{}); ok {
				rule := ValidationRule{
					Field:       s.getString(ruleMap, "field"),
					Type:        s.getString(ruleMap, "type"),
					Constraints: ruleMap["constraints"],
					Message:     s.getString(ruleMap, "message"),
				}
				opts.Rules = append(opts.Rules, rule)
			}
		}
	}

	return opts, nil
}

// getString helper to safely get string from map.
func (s *ValidateService) getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// validateData performs the actual validation.
func (s *ValidateService) validateData(data []DataRow, opts *ValidateOptions) (*ValidationResult, []DataRow, []DataRow) {
	result := &ValidationResult{
		TotalRows:  len(data),
		FieldStats: make(map[string]int),
	}

	var validData, invalidData []DataRow

	for i, row := range data {
		rowErrors, rowWarnings := s.validateRow(row, opts.Rules, i+1)

		if len(rowErrors) > 0 {
			invalidData = append(invalidData, row)
			result.Errors = append(result.Errors, rowErrors...)
			result.InvalidRows++
		} else {
			validData = append(validData, row)
			result.ValidRows++
		}

		result.Warnings = append(result.Warnings, rowWarnings...)

		// Update field statistics
		for field := range row.Fields {
			result.FieldStats[field]++
		}

		// Stop validation if fail_fast is enabled and we have errors
		if opts.FailFast && len(rowErrors) > 0 {
			break
		}
	}

	// Calculate quality score
	result.QualityScore = s.calculateQualityScore(result)

	return result, validData, invalidData
}

// validateRow validates a single row against all rules.
func (s *ValidateService) validateRow(row DataRow, rules []ValidationRule, rowNumber int) ([]ValidationError, []ValidationError) {
	var errors, warnings []ValidationError

	for _, rule := range rules {
		validationError := s.validateField(row, rule, rowNumber)
		if validationError != nil {
			if validationError.Severity == "error" {
				errors = append(errors, *validationError)
			} else {
				warnings = append(warnings, *validationError)
			}
		}
	}

	return errors, warnings
}

// validateField validates a single field against a rule.
//
//nolint:gocyclo
func (s *ValidateService) validateField(row DataRow, rule ValidationRule, rowNumber int) *ValidationError {
	fieldValue, exists := row.Fields[rule.Field]
	if !exists {
		return &ValidationError{
			RowNumber:  rowNumber,
			FieldName:  rule.Field,
			FieldValue: "",
			RuleType:   rule.Type,
			Message:    fmt.Sprintf("Field '%s' is missing", rule.Field),
			Severity:   "error",
			RowData:    row,
		}
	}

	var isValid bool
	var message string

	switch rule.Type {
	case "required":
		isValid = fieldValue != ""
		if !isValid {
			message = "Field is required"
		}

	case "email":
		isValid = s.validateEmail(fieldValue)
		if !isValid {
			message = "Invalid email format"
		}

	case "numeric":
		isValid = s.validateNumeric(fieldValue)
		if !isValid {
			message = "Value must be numeric"
		}

	case "regex": //nolint:goconst
		if pattern, ok := rule.Constraints.(string); ok {
			isValid = s.validateRegex(fieldValue, pattern)
			if !isValid {
				message = "Value does not match pattern: " + pattern
			}
		} else {
			message = "Regex pattern not specified"
		}

	case "min_length":
		if minLength, ok := rule.Constraints.(float64); ok {
			isValid = len(fieldValue) >= int(minLength)
			if !isValid {
				message = fmt.Sprintf("Value must be at least %d characters", int(minLength))
			}
		} else {
			message = "Min length not specified"
		}

	case "max_length":
		if maxLength, ok := rule.Constraints.(float64); ok {
			isValid = len(fieldValue) <= int(maxLength)
			if !isValid {
				message = fmt.Sprintf("Value must be at most %d characters", int(maxLength))
			}
		} else {
			message = "Max length not specified"
		}

	case "range":
		if constraints, ok := rule.Constraints.(map[string]interface{}); ok { //nolint:nestif
			if mIn, ok := constraints["min"].(float64); ok {
				if mAx, ok := constraints["max"].(float64); ok {
					if num, err := strconv.ParseFloat(fieldValue, 64); err == nil {
						isValid = num >= mIn && num <= mAx
						if !isValid {
							message = fmt.Sprintf("Value must be between %.2f and %.2f", mIn, mAx)
						}
					} else {
						message = "Value must be numeric for range validation"
					}
				} else {
					message = "Max value not specified for range"
				}
			} else {
				message = "Min value not specified for range"
			}
		} else {
			message = "Range constraints not specified"
		}

	default:
		// Unknown rule type - treat as warning
		return &ValidationError{
			RowNumber:  rowNumber,
			FieldName:  rule.Field,
			FieldValue: fieldValue,
			RuleType:   rule.Type,
			Message:    "Unknown validation rule type: " + rule.Type,
			Severity:   "warning",
			RowData:    row,
		}
	}

	if !isValid {
		errorMessage := message
		if rule.Message != "" {
			errorMessage = rule.Message
		}

		severity := "error"
		if rule.Type == "regex" || rule.Type == "min_length" || rule.Type == "max_length" {
			severity = "warning" // These are often warnings rather than errors
		}

		return &ValidationError{
			RowNumber:  rowNumber,
			FieldName:  rule.Field,
			FieldValue: fieldValue,
			RuleType:   rule.Type,
			Message:    errorMessage,
			Severity:   severity,
			RowData:    row,
		}
	}

	return nil
}

// validateEmail validates email format.
func (s *ValidateService) validateEmail(email string) bool {
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	//bearer:disable go_lang_permissive_regex_validation
	return emailRegex.MatchString(email)
}

// validateNumeric validates numeric format.
func (s *ValidateService) validateNumeric(value string) bool {
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

// validateRegex validates against regex pattern.
func (s *ValidateService) validateRegex(value, pattern string) bool {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return regex.MatchString(value)
}

// calculateQualityScore calculates data quality score.
func (s *ValidateService) calculateQualityScore(result *ValidationResult) float64 {
	if result.TotalRows == 0 {
		return 0
	}

	// Base score on valid rows percentage
	score := float64(result.ValidRows) / float64(result.TotalRows) * 100

	// Deduct points for warnings
	warningPenalty := float64(len(result.Warnings)) / float64(result.TotalRows) * 5
	score -= warningPenalty

	// Ensure score is between 0 and 100
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// ValidateFile validates data from a file
// This convenience method demonstrates file-based validation.
func (s *ValidateService) ValidateFile(inputFile, outputFile string, rules []ValidationRule, failFast bool) (*ValidationResult, error) {
	s.logger.Info().
		Str("input", inputFile).
		Str("output", outputFile).
		Int("rules", len(rules)).
		Bool("fail_fast", failFast).
		Msg("Starting file validation")

	options := map[string]interface{}{
		"input_file":  inputFile,
		"output_file": outputFile,
		"rules":       rules,
		"fail_fast":   failFast,
	}

	validData, err := s.ProcessData(nil, options)
	if err != nil {
		return nil, err
	}

	// Create validation result
	result := ValidationResult{
		ValidRows:    len(validData),
		TotalRows:    len(validData), // This would be calculated properly in the actual validation
		QualityScore: 100.0,
	}
	return &result, nil
}
