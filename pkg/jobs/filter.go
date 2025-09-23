package jobs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// FilterService handles data filtering operations
// This service demonstrates conditional data processing with dependency injection.
type FilterService struct {
	fileService *FileService   `do:""`
	logger      zerolog.Logger `do:""`
}

// NewFilterService creates a new filter service with dependency injection.
func NewFilterService(i do.Injector) (*FilterService, error) {
	return &FilterService{
		fileService: do.MustInvoke[*FileService](i),
		logger:      do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// FilterRule represents a filtering rule.
type FilterRule struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// FilterOptions contains filtering configuration.
type FilterOptions struct {
	InputFile  string       `json:"input_file"`
	OutputFile string       `json:"output_file"`
	Rules      []FilterRule `json:"rules"`
	Inclusive  bool         `json:"inclusive"` // true = keep matches, false = remove matches
}

// ProcessData filters data based on rules
// This method demonstrates complex data filtering logic.
func (s *FilterService) ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error) {
	s.logger.Info().Msg("Filtering data based on rules")

	// Parse options
	opts, err := s.parseFilterOptions(options)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter options: %w", err)
	}

	// If input data is empty, try to read from file
	if len(input) == 0 && opts.InputFile != "" {
		var err error
		input, err = s.fileService.ReadCSV(opts.InputFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read input file: %w", err)
		}
	}

	var filteredData []DataRow

	// Apply each filter rule to each row
	for _, row := range input {
		matches := s.matchesAllRules(row, opts.Rules)

		// Include row based on inclusive setting
		if (opts.Inclusive && matches) || (!opts.Inclusive && !matches) {
			filteredData = append(filteredData, row)
		}
	}

	// Write results to file if output file specified
	if opts.OutputFile != "" {
		if err := s.fileService.WriteJSON(opts.OutputFile, filteredData); err != nil {
			return nil, fmt.Errorf("failed to write filtered data: %w", err)
		}
	}

	s.logger.Info().
		Int("input_records", len(input)).
		Int("output_records", len(filteredData)).
		Int("rules", len(opts.Rules)).
		Msg("Data filtering completed")

	return filteredData, nil
}

// GetName returns the processor name.
func (s *FilterService) GetName() string {
	return "filter-data"
}

// GetDescription returns the processor description.
func (s *FilterService) GetDescription() string {
	return "Filter data based on field conditions"
}

// parseFilterOptions parses filter options from map.
func (s *FilterService) parseFilterOptions(options map[string]interface{}) (*FilterOptions, error) {
	opts := &FilterOptions{
		Inclusive: true, // default to inclusive filtering
	}

	if inputFile, ok := options["input_file"].(string); ok {
		opts.InputFile = inputFile
	}

	if outputFile, ok := options["output_file"].(string); ok {
		opts.OutputFile = outputFile
	}

	if inclusive, ok := options["inclusive"].(bool); ok {
		opts.Inclusive = inclusive
	}

	// Parse filter rules
	if rulesRaw, ok := options["rules"].([]interface{}); ok {
		for _, ruleRaw := range rulesRaw {
			if ruleMap, ok := ruleRaw.(map[string]interface{}); ok {
				rule := FilterRule{
					Field:    s.getString(ruleMap, "field"),
					Operator: s.getString(ruleMap, "operator"),
				}

				if val, ok := ruleMap["value"]; ok {
					rule.Value = val
				}

				opts.Rules = append(opts.Rules, rule)
			}
		}
	}

	return opts, nil
}

// getString helper to safely get string from map.
func (s *FilterService) getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// matchesAllRules checks if a row matches all filter rules.
func (s *FilterService) matchesAllRules(row DataRow, rules []FilterRule) bool {
	for _, rule := range rules {
		if !s.matchesRule(row, rule) {
			return false
		}
	}
	return true
}

// matchesRule checks if a row matches a single filter rule.
func (s *FilterService) matchesRule(row DataRow, rule FilterRule) bool {
	fieldValue, exists := row.Fields[rule.Field]
	if !exists {
		return false
	}

	switch rule.Operator {
	case "equals":
		return s.compareValues(fieldValue, rule.Value)
	case "not_equals":
		return !s.compareValues(fieldValue, rule.Value)
	case "contains":
		return strings.Contains(strings.ToLower(fieldValue), strings.ToLower(fmt.Sprintf("%v", rule.Value)))
	case "not_contains":
		return !strings.Contains(strings.ToLower(fieldValue), strings.ToLower(fmt.Sprintf("%v", rule.Value)))
	case "starts_with":
		return strings.HasPrefix(strings.ToLower(fieldValue), strings.ToLower(fmt.Sprintf("%v", rule.Value)))
	case "ends_with":
		return strings.HasSuffix(strings.ToLower(fieldValue), strings.ToLower(fmt.Sprintf("%v", rule.Value)))
	case "regex":
		if pattern, ok := rule.Value.(string); ok {
			matched, err := regexp.MatchString(pattern, fieldValue)
			return err == nil && matched
		}
		return false
	case "greater_than":
		return s.numericCompare(fieldValue, rule.Value, true)
	case "less_than":
		return s.numericCompare(fieldValue, rule.Value, false)
	default:
		s.logger.Warn().Str("operator", rule.Operator).Msg("Unknown filter operator")
		return false
	}
}

// compareValues compares two values with type conversion.
func (s *FilterService) compareValues(a string, b interface{}) bool {
	switch v := b.(type) {
	case string:
		return strings.EqualFold(a, v)
	case int, int64, float64:
		// Try to convert string to number
		if num, err := strconv.ParseFloat(a, 64); err == nil { //nolint:nestif
			if strVal, ok := v.(string); ok {
				if strNum, err := strconv.ParseFloat(strVal, 64); err == nil {
					return num == strNum
				}
			} else if numVal, ok := v.(float64); ok {
				return num == numVal
			} else if intVal, ok := v.(int); ok {
				return num == float64(intVal)
			}
		}
		return false
	default:
		return a == fmt.Sprintf("%v", v)
	}
}

// numericCompare performs numeric comparison.
func (s *FilterService) numericCompare(a string, b interface{}, greater bool) bool {
	aNum, err1 := strconv.ParseFloat(a, 64)
	var bNum float64

	switch v := b.(type) {
	case float64:
		bNum = v
	case int:
		bNum = float64(v)
	case string:
		var err2 error
		bNum, err2 = strconv.ParseFloat(v, 64)
		if err2 != nil {
			return false
		}
	default:
		return false
	}

	if err1 != nil {
		return false
	}

	if greater {
		return aNum > bNum
	}
	return aNum < bNum
}

// FilterByFile filters data from a file using filter rules
// This convenience method demonstrates file-based filtering.
func (s *FilterService) FilterByFile(inputFile, outputFile string, rules []FilterRule, inclusive bool) (*ProcessingResult, error) {
	s.logger.Info().
		Str("input", inputFile).
		Str("output", outputFile).
		Int("rules", len(rules)).
		Bool("inclusive", inclusive).
		Msg("Starting file filtering")

	options := map[string]interface{}{
		"input_file":  inputFile,
		"output_file": outputFile,
		"rules":       rules,
		"inclusive":   inclusive,
	}

	filteredData, err := s.ProcessData(nil, options)
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
		Processed:  len(filteredData),
		OutputPath: outputFile,
		Processor:  s.GetName(),
	}, nil
}
