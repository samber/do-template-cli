package jobs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// TransformOperation defines transformation operation types.
type TransformOperation string

const (
	UpperCase   TransformOperation = "upper_case"
	LowerCase   TransformOperation = "lower_case"
	TitleCase   TransformOperation = "title_case"
	Trim        TransformOperation = "trim"
	Replace     TransformOperation = "replace"
	Extract     TransformOperation = "extract"
	Split       TransformOperation = "split"
	Join        TransformOperation = "join"
	FormatDate  TransformOperation = "format_date"
	Calculate   TransformOperation = "calculate"
	Conditional TransformOperation = "conditional"
)

// TransformRule defines a transformation rule.
type TransformRule struct {
	Field       string                 `json:"field"`
	Operation   TransformOperation     `json:"operation"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	TargetField string                 `json:"target_field,omitempty"` // if different from source
}

// TransformOptions contains transformation configuration.
type TransformOptions struct {
	InputFile  string          `json:"input_file"`
	OutputFile string          `json:"output_file"`
	Rules      []TransformRule `json:"rules"`
	KeepFields bool            `json:"keep_fields"` // keep non-transformed fields
	DropNulls  bool            `json:"drop_nulls"`  // remove rows with null values after transformation
}

// TransformService handles data transformation operations
// This service demonstrates data field transformation with dependency injection.
type TransformService struct {
	fileService *FileService   `do:""`
	logger      zerolog.Logger `do:""`
}

// NewTransformService creates a new transform service with dependency injection.
func NewTransformService(i do.Injector) (*TransformService, error) {
	return &TransformService{
		fileService: do.MustInvoke[*FileService](i),
		logger:      do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// ProcessData transforms data based on rules
// This method demonstrates comprehensive data transformation logic.
func (s *TransformService) ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error) {
	s.logger.Info().Msg("Transforming data based on rules")

	// Parse options
	opts, err := s.parseTransformOptions(options)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transform options: %w", err)
	}

	// If input data is empty, try to read from file
	if len(input) == 0 && opts.InputFile != "" {
		var err error
		input, err = s.fileService.ReadCSV(opts.InputFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read input file: %w", err)
		}
	}

	// Perform transformations
	transformedData := s.transformData(input, opts)

	// Filter out null rows if requested
	if opts.DropNulls {
		transformedData = s.filterNullRows(transformedData)
	}

	// Write results to file if output file specified
	if opts.OutputFile != "" {
		if err := s.fileService.WriteJSON(opts.OutputFile, transformedData); err != nil {
			return nil, fmt.Errorf("failed to write transformed data: %w", err)
		}
	}

	s.logger.Info().
		Int("input_records", len(input)).
		Int("output_records", len(transformedData)).
		Int("rules", len(opts.Rules)).
		Msg("Data transformation completed")

	return transformedData, nil
}

// GetName returns the processor name.
func (s *TransformService) GetName() string {
	return "transform-data"
}

// GetDescription returns the processor description.
func (s *TransformService) GetDescription() string {
	return "Transform data fields with various operations"
}

// parseTransformOptions parses transformation options from map.
func (s *TransformService) parseTransformOptions(options map[string]interface{}) (*TransformOptions, error) {
	opts := &TransformOptions{
		KeepFields: true, // default to keeping all fields
	}

	if inputFile, ok := options["input_file"].(string); ok {
		opts.InputFile = inputFile
	}

	if outputFile, ok := options["output_file"].(string); ok {
		opts.OutputFile = outputFile
	}

	if keepFields, ok := options["keep_fields"].(bool); ok {
		opts.KeepFields = keepFields
	}

	if dropNulls, ok := options["drop_nulls"].(bool); ok {
		opts.DropNulls = dropNulls
	}

	// Parse transformation rules
	if rulesRaw, ok := options["rules"].([]interface{}); ok {
		for _, ruleRaw := range rulesRaw {
			if ruleMap, ok := ruleRaw.(map[string]interface{}); ok {
				rule := TransformRule{
					Field:       s.getString(ruleMap, "field"),
					Operation:   TransformOperation(s.getString(ruleMap, "operation")),
					TargetField: s.getString(ruleMap, "target_field"),
				}

				if params, ok := ruleMap["parameters"].(map[string]interface{}); ok {
					rule.Parameters = params
				}

				opts.Rules = append(opts.Rules, rule)
			}
		}
	}

	return opts, nil
}

// getString helper to safely get string from map.
func (s *TransformService) getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// transformData performs the actual transformations.
func (s *TransformService) transformData(data []DataRow, opts *TransformOptions) []DataRow {
	transformedData := []DataRow{}

	for _, row := range data {
		transformedRow := s.transformRow(row, opts)
		transformedData = append(transformedData, transformedRow)
	}

	return transformedData
}

// transformRow transforms a single row based on rules.
func (s *TransformService) transformRow(row DataRow, opts *TransformOptions) DataRow {
	transformedRow := DataRow{Fields: make(map[string]string)}

	// Copy original fields if keeping fields
	if opts.KeepFields {
		for field, value := range row.Fields {
			transformedRow.Fields[field] = value
		}
	}

	// Apply transformation rules
	for _, rule := range opts.Rules {
		result := s.applyTransformRule(row, rule)
		targetField := rule.TargetField
		if targetField == "" {
			targetField = rule.Field
		}
		transformedRow.Fields[targetField] = result
	}

	return transformedRow
}

// applyTransformRule applies a single transformation rule.
func (s *TransformService) applyTransformRule(row DataRow, rule TransformRule) string {
	fieldValue, exists := row.Fields[rule.Field]
	if !exists {
		return ""
	}

	//nolint:exhaustive
	switch rule.Operation {
	case UpperCase:
		return strings.ToUpper(fieldValue)
	case LowerCase:
		return strings.ToLower(fieldValue)
	case TitleCase:
		return strings.Title(strings.ToLower(fieldValue)) //nolint:staticcheck
	case Trim:
		return strings.TrimSpace(fieldValue)
	case Replace:
		return s.applyReplace(fieldValue, rule.Parameters)
	case Extract:
		return s.applyExtract(fieldValue, rule.Parameters)
	case Split:
		return s.applySplit(fieldValue, rule.Parameters)
	case Join:
		return s.applyJoin(fieldValue, rule.Parameters)
	case Calculate:
		return s.applyCalculate(fieldValue, rule.Parameters)
	case Conditional:
		return s.applyConditional(row, rule.Parameters)
	default:
		s.logger.Warn().Str("operation", string(rule.Operation)).Msg("Unknown transform operation")
		return fieldValue
	}
}

// applyReplace applies string replacement.
func (s *TransformService) applyReplace(value string, params map[string]interface{}) string {
	oldStr, ok := params["old"].(string)
	if !ok {
		return value
	}
	newStr, ok := params["new"].(string)
	if !ok {
		newStr = ""
	}
	return strings.ReplaceAll(value, oldStr, newStr)
}

// applyExtract extracts text using regex.
func (s *TransformService) applyExtract(value string, params map[string]interface{}) string {
	pattern, ok := params["pattern"].(string)
	if !ok {
		return value
	}
	group, ok := params["group"].(float64)
	if !ok {
		group = 0
	}

	regex, err := regexp.Compile(pattern)
	if err != nil {
		s.logger.Error().Err(err).Str("pattern", pattern).Msg("Invalid regex pattern")
		return value
	}

	matches := regex.FindStringSubmatch(value)
	if len(matches) > int(group) {
		return matches[int(group)]
	}

	return value
}

// applySplit splits string and optionally joins back.
func (s *TransformService) applySplit(value string, params map[string]interface{}) string {
	separator, ok := params["separator"].(string)
	if !ok {
		separator = ","
	}
	joinWith, ok := params["join_with"].(string)
	if !ok {
		joinWith = ""
	}

	parts := strings.Split(value, separator)
	if joinWith != "" {
		return strings.Join(parts, joinWith)
	}

	// If no join specified, return first part
	if len(parts) > 0 {
		return parts[0]
	}
	return value
}

// applyJoin joins array elements (simulated).
func (s *TransformService) applyJoin(value string, params map[string]interface{}) string {
	separator, ok := params["separator"].(string)
	if !ok {
		separator = ","
	}

	// Treat value as a simple array-like string (e.g., "a,b,c")
	parts := strings.Split(value, ",")
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}

	return strings.Join(parts, separator)
}

// applyCalculate performs mathematical calculations.
func (s *TransformService) applyCalculate(value string, params map[string]interface{}) string {
	operation, ok := params["operation"].(string)
	if !ok {
		return value
	}

	operand, ok := params["operand"].(float64)
	if !ok {
		return value
	}

	numValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		s.logger.Error().Err(err).Str("value", value).Msg("Cannot parse numeric value")
		return value
	}

	switch operation {
	case "add":
		return fmt.Sprintf("%.2f", numValue+operand)
	case "subtract":
		return fmt.Sprintf("%.2f", numValue-operand)
	case "multiply":
		return fmt.Sprintf("%.2f", numValue*operand)
	case "divide":
		if operand != 0 {
			return fmt.Sprintf("%.2f", numValue/operand)
		}
		return "0"
	default:
		return value
	}
}

// applyConditional applies conditional logic.
//
//nolint:gocyclo
func (s *TransformService) applyConditional(row DataRow, params map[string]interface{}) string {
	field, ok := params["field"].(string)
	if !ok {
		return ""
	}

	operator, ok := params["operator"].(string)
	if !ok {
		return ""
	}

	value, ok := params["value"].(string)
	if !ok {
		return ""
	}

	trueResult, ok := params["true_result"].(string)
	if !ok {
		trueResult = "true"
	}

	falseResult, ok := params["false_result"].(string)
	if !ok {
		falseResult = "false"
	}

	fieldValue, exists := row.Fields[field]
	if !exists {
		return falseResult
	}

	switch operator {
	case "equals":
		if fieldValue == value {
			return trueResult
		}
	case "not_equals":
		if fieldValue != value {
			return trueResult
		}
	case "contains":
		if strings.Contains(fieldValue, value) {
			return trueResult
		}
	case "greater_than":
		if num1, err1 := strconv.ParseFloat(fieldValue, 64); err1 == nil {
			if num2, err2 := strconv.ParseFloat(value, 64); err2 == nil {
				if num1 > num2 {
					return trueResult
				}
			}
		}
	case "less_than":
		if num1, err1 := strconv.ParseFloat(fieldValue, 64); err1 == nil {
			if num2, err2 := strconv.ParseFloat(value, 64); err2 == nil {
				if num1 < num2 {
					return trueResult
				}
			}
		}
	}

	return falseResult
}

// filterNullRows removes rows with null/empty values.
func (s *TransformService) filterNullRows(data []DataRow) []DataRow {
	var filteredData []DataRow

	for _, row := range data {
		hasNull := false
		for _, value := range row.Fields {
			if value == "" {
				hasNull = true
				break
			}
		}
		if !hasNull {
			filteredData = append(filteredData, row)
		}
	}

	return filteredData
}

// TransformFile transforms data from a file
// This convenience method demonstrates file-based transformation.
func (s *TransformService) TransformFile(inputFile, outputFile string, rules []TransformRule, keepFields bool) (*ProcessingResult, error) {
	s.logger.Info().
		Str("input", inputFile).
		Str("output", outputFile).
		Int("rules", len(rules)).
		Bool("keep_fields", keepFields).
		Msg("Starting file transformation")

	options := map[string]interface{}{
		"input_file":  inputFile,
		"output_file": outputFile,
		"rules":       rules,
		"keep_fields": keepFields,
	}

	transformedData, err := s.ProcessData(nil, options)
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
		Processed:  len(transformedData),
		OutputPath: outputFile,
		Processor:  s.GetName(),
	}, nil
}
