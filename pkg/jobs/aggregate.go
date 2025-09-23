package jobs

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samber/do/v2"
)

// AggregateService handles data aggregation operations
// This service demonstrates data summarization and statistical analysis with dependency injection.
type AggregateService struct {
	fileService *FileService   `do:""`
	logger      zerolog.Logger `do:""`
}

// NewAggregateService creates a new aggregate service with dependency injection.
func NewAggregateService(i do.Injector) (*AggregateService, error) {
	return &AggregateService{
		fileService: do.MustInvoke[*FileService](i),
		logger:      do.MustInvoke[zerolog.Logger](i),
	}, nil
}

// AggregateOperation defines aggregation operation types.
type AggregateOperation string

const (
	Count    AggregateOperation = "count"
	Sum      AggregateOperation = "sum"
	Average  AggregateOperation = "average"
	Min      AggregateOperation = "min"
	Max      AggregateOperation = "max"
	GroupBy  AggregateOperation = "group_by"
	Distinct AggregateOperation = "distinct"
)

// AggregateRule defines an aggregation rule.
type AggregateRule struct {
	Field     string             `json:"field"`
	Operation AggregateOperation `json:"operation"`
	Alias     string             `json:"alias,omitempty"`
}

// AggregateOptions contains aggregation configuration.
type AggregateOptions struct {
	InputFile  string          `json:"input_file"`
	OutputFile string          `json:"output_file"`
	Rules      []AggregateRule `json:"rules"`
	GroupBy    []string        `json:"group_by,omitempty"`
	SortBy     string          `json:"sort_by,omitempty"`
	SortDesc   bool            `json:"sort_desc,omitempty"`
}

// AggregateResult represents the result of an aggregation operation.
type AggregateResult struct {
	Groups    []GroupResult  `json:"groups,omitempty"`
	Summary   *SummaryResult `json:"summary,omitempty"`
	TotalRows int            `json:"total_rows"`
}

// GroupResult represents aggregated data for a group.
type GroupResult struct {
	GroupKey    string                 `json:"group_key"`
	GroupValues map[string]string      `json:"group_values,omitempty"`
	Aggregates  map[string]interface{} `json:"aggregates"`
	Count       int                    `json:"count"`
}

// SummaryResult represents overall summary statistics.
type SummaryResult struct {
	TotalRecords int                   `json:"total_records"`
	FieldStats   map[string]FieldStats `json:"field_stats,omitempty"`
}

// FieldStats contains statistical information for a field.
type FieldStats struct {
	Count     int64   `json:"count"`
	Sum       float64 `json:"sum,omitempty"`
	Average   float64 `json:"average,omitempty"`
	Min       float64 `json:"min,omitempty"`
	Max       float64 `json:"max,omitempty"`
	Unique    int64   `json:"unique,omitempty"`
	NullCount int64   `json:"null_count,omitempty"`
}

// ProcessData performs aggregation operations on data
// This method demonstrates complex data aggregation logic.
func (s *AggregateService) ProcessData(input []DataRow, options map[string]interface{}) ([]DataRow, error) {
	s.logger.Info().Msg("Performing data aggregation")

	// Parse options
	opts, err := s.parseAggregateOptions(options)
	if err != nil {
		return nil, fmt.Errorf("failed to parse aggregate options: %w", err)
	}

	// If input data is empty, try to read from file
	if len(input) == 0 && opts.InputFile != "" {
		var err error
		input, err = s.fileService.ReadCSV(opts.InputFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read input file: %w", err)
		}
	}

	// Perform aggregation
	result, err := s.aggregateData(input, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate data: %w", err)
	}

	// Write results to file if output file specified
	if opts.OutputFile != "" {
		if err := s.fileService.WriteJSON(opts.OutputFile, result); err != nil {
			return nil, fmt.Errorf("failed to write aggregated data: %w", err)
		}
	}

	// Convert result back to DataRow format for consistency
	return s.convertResultToDataRows(result), nil
}

// GetName returns the processor name.
func (s *AggregateService) GetName() string {
	return "aggregate-data"
}

// GetDescription returns the processor description.
func (s *AggregateService) GetDescription() string {
	return "Aggregate and summarize data with statistical operations"
}

// parseAggregateOptions parses aggregation options from map.
func (s *AggregateService) parseAggregateOptions(options map[string]interface{}) (*AggregateOptions, error) {
	opts := &AggregateOptions{}

	if inputFile, ok := options["input_file"].(string); ok {
		opts.InputFile = inputFile
	}

	if outputFile, ok := options["output_file"].(string); ok {
		opts.OutputFile = outputFile
	}

	// Parse aggregation rules
	if rulesRaw, ok := options["rules"].([]interface{}); ok {
		for _, ruleRaw := range rulesRaw {
			if ruleMap, ok := ruleRaw.(map[string]interface{}); ok {
				rule := AggregateRule{
					Field:     s.getString(ruleMap, "field"),
					Operation: AggregateOperation(s.getString(ruleMap, "operation")),
					Alias:     s.getString(ruleMap, "alias"),
				}
				opts.Rules = append(opts.Rules, rule)
			}
		}
	}

	// Parse group by fields
	if groupByRaw, ok := options["group_by"].([]interface{}); ok {
		for _, field := range groupByRaw {
			if fieldStr, ok := field.(string); ok {
				opts.GroupBy = append(opts.GroupBy, fieldStr)
			}
		}
	}

	if sortBy, ok := options["sort_by"].(string); ok {
		opts.SortBy = sortBy
	}

	if sortDesc, ok := options["sort_desc"].(bool); ok {
		opts.SortDesc = sortDesc
	}

	return opts, nil
}

// getString helper to safely get string from map.
func (s *AggregateService) getString(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// aggregateData performs the actual aggregation.
func (s *AggregateService) aggregateData(data []DataRow, opts *AggregateOptions) (*AggregateResult, error) {
	result := &AggregateResult{
		TotalRows: len(data),
	}

	if len(opts.GroupBy) > 0 {
		// Group by aggregation
		groups := s.groupData(data, opts.GroupBy)
		result.Groups = s.processGroups(groups, opts)
	} else {
		// Overall aggregation
		summary := s.processOverallAggregation(data, opts)
		result.Summary = summary
	}

	return result, nil
}

// groupData groups data by specified fields.
func (s *AggregateService) groupData(data []DataRow, groupBy []string) map[string][]DataRow {
	groups := make(map[string][]DataRow)

	for _, row := range data {
		key := s.createGroupKey(row, groupBy)
		groups[key] = append(groups[key], row)
	}

	return groups
}

// createGroupKey creates a unique key for grouping.
func (s *AggregateService) createGroupKey(row DataRow, groupBy []string) string {
	keyParts := []string{}
	for _, field := range groupBy {
		keyParts = append(keyParts, row.Fields[field])
	}
	return strings.Join(keyParts, "|")
}

// processGroups processes each group with aggregation rules.
func (s *AggregateService) processGroups(groups map[string][]DataRow, opts *AggregateOptions) []GroupResult {
	groupResults := []GroupResult{}

	for key, groupData := range groups {
		groupResult := GroupResult{
			GroupKey:   key,
			Count:      len(groupData),
			Aggregates: make(map[string]interface{}),
		}

		// Set group values
		if len(groupData) > 0 {
			groupResult.GroupValues = make(map[string]string)
			for _, field := range opts.GroupBy {
				groupResult.GroupValues[field] = groupData[0].Fields[field]
			}
		}

		// Apply aggregation rules
		for _, rule := range opts.Rules {
			result := s.applyAggregateRule(groupData, rule)
			alias := rule.Alias
			if alias == "" {
				alias = fmt.Sprintf("%s_%s", rule.Field, rule.Operation)
			}
			groupResult.Aggregates[alias] = result
		}

		groupResults = append(groupResults, groupResult)
	}

	// Sort results if specified
	if opts.SortBy != "" {
		s.sortGroupResults(groupResults, opts.SortBy, opts.SortDesc)
	}

	return groupResults
}

// processOverallAggregation processes overall aggregation without grouping.
func (s *AggregateService) processOverallAggregation(data []DataRow, opts *AggregateOptions) *SummaryResult {
	summary := &SummaryResult{
		TotalRecords: len(data),
		FieldStats:   make(map[string]FieldStats),
	}

	// Apply aggregation rules
	for _, rule := range opts.Rules {
		//nolint:exhaustive
		switch rule.Operation {
		case Count:
			summary.FieldStats[rule.Field] = FieldStats{
				Count: int64(len(data)),
			}
		default:
			stats := s.calculateFieldStats(data, rule.Field)
			summary.FieldStats[rule.Field] = stats
		}
	}

	return summary
}

// applyAggregateRule applies a single aggregation rule to a group.
func (s *AggregateService) applyAggregateRule(groupData []DataRow, rule AggregateRule) interface{} {
	//nolint:exhaustive
	switch rule.Operation {
	case Count:
		return len(groupData)
	case Sum:
		return s.calculateSum(groupData, rule.Field)
	case Average:
		return s.calculateAverage(groupData, rule.Field)
	case Min:
		return s.calculateMin(groupData, rule.Field)
	case Max:
		return s.calculateMax(groupData, rule.Field)
	case Distinct:
		return s.calculateDistinct(groupData, rule.Field)
	default:
		return nil
	}
}

// calculateSum calculates the sum of numeric values in a field.
func (s *AggregateService) calculateSum(data []DataRow, field string) float64 {
	var sum float64
	for _, row := range data {
		if val, err := strconv.ParseFloat(row.Fields[field], 64); err == nil {
			sum += val
		}
	}
	return sum
}

// calculateAverage calculates the average of numeric values in a field.
func (s *AggregateService) calculateAverage(data []DataRow, field string) float64 {
	if len(data) == 0 {
		return 0
	}
	return s.calculateSum(data, field) / float64(len(data))
}

// calculateMin calculates the minimum value in a field.
func (s *AggregateService) calculateMin(data []DataRow, field string) float64 {
	if len(data) == 0 {
		return 0
	}
	mIn := math.MaxFloat64
	for _, row := range data {
		if val, err := strconv.ParseFloat(row.Fields[field], 64); err == nil && val < mIn {
			mIn = val
		}
	}
	return mIn
}

// calculateMax calculates the maximum value in a field.
func (s *AggregateService) calculateMax(data []DataRow, field string) float64 {
	if len(data) == 0 {
		return 0
	}
	mAx := -math.MaxFloat64
	for _, row := range data {
		if val, err := strconv.ParseFloat(row.Fields[field], 64); err == nil && val > mAx {
			mAx = val
		}
	}
	return mAx
}

// calculateDistinct calculates the number of distinct values in a field.
func (s *AggregateService) calculateDistinct(data []DataRow, field string) int64 {
	unique := make(map[string]bool)
	for _, row := range data {
		unique[row.Fields[field]] = true
	}
	return int64(len(unique))
}

// calculateFieldStats calculates comprehensive statistics for a field.
func (s *AggregateService) calculateFieldStats(data []DataRow, field string) FieldStats {
	stats := FieldStats{
		Count: int64(len(data)),
	}

	var sum float64
	var numericValues []float64
	unique := make(map[string]bool)
	nullCount := 0

	for _, row := range data {
		value := row.Fields[field]
		if value == "" {
			nullCount++
			continue
		}

		unique[value] = true

		if val, err := strconv.ParseFloat(value, 64); err == nil {
			numericValues = append(numericValues, val)
			sum += val
		}
	}

	stats.Unique = int64(len(unique))
	stats.NullCount = int64(nullCount)

	if len(numericValues) > 0 {
		stats.Sum = sum
		stats.Average = sum / float64(len(numericValues))
		stats.Min = numericValues[0]
		stats.Max = numericValues[0]

		for _, val := range numericValues {
			if val < stats.Min {
				stats.Min = val
			}
			if val > stats.Max {
				stats.Max = val
			}
		}
	}

	return stats
}

// sortGroupResults sorts group results.
func (s *AggregateService) sortGroupResults(groups []GroupResult, sortBy string, desc bool) {
	sort.Slice(groups, func(i, j int) bool {
		var iVal, jVal float64
		var iOk, jOk bool

		if val, ok := groups[i].Aggregates[sortBy].(float64); ok {
			iVal = val
			iOk = true
		} else if val, ok := groups[i].Aggregates[sortBy].(int); ok {
			iVal = float64(val)
			iOk = true
		}

		if val, ok := groups[j].Aggregates[sortBy].(float64); ok {
			jVal = val
			jOk = true
		} else if val, ok := groups[j].Aggregates[sortBy].(int); ok {
			jVal = float64(val)
			jOk = true
		}

		if !iOk || !jOk {
			return false
		}

		if desc {
			return iVal > jVal
		}
		return iVal < jVal
	})
}

// convertResultToDataRows converts AggregateResult to DataRow format.
func (s *AggregateService) convertResultToDataRows(result *AggregateResult) []DataRow {
	var rows []DataRow

	if len(result.Groups) > 0 {
		for _, group := range result.Groups {
			row := DataRow{Fields: make(map[string]string)}

			// Add group key and values
			row.Fields["group_key"] = group.GroupKey
			row.Fields["count"] = strconv.Itoa(group.Count)

			// Add group values
			for field, value := range group.GroupValues {
				row.Fields[field] = value
			}

			// Add aggregates
			for alias, value := range group.Aggregates {
				row.Fields[alias] = fmt.Sprintf("%v", value)
			}

			rows = append(rows, row)
		}
	} else if result.Summary.TotalRecords > 0 {
		row := DataRow{Fields: make(map[string]string)}
		row.Fields["total_records"] = strconv.Itoa(result.Summary.TotalRecords)

		// Add summary statistics
		for field, stats := range result.Summary.FieldStats {
			row.Fields[field+"_count"] = strconv.FormatInt(stats.Count, 10)
			if stats.Sum != 0 {
				row.Fields[field+"_sum"] = fmt.Sprintf("%.2f", stats.Sum)
				row.Fields[field+"_average"] = fmt.Sprintf("%.2f", stats.Average)
				row.Fields[field+"_min"] = fmt.Sprintf("%.2f", stats.Min)
				row.Fields[field+"_max"] = fmt.Sprintf("%.2f", stats.Max)
			}
			if stats.Unique != 0 {
				row.Fields[field+"_unique"] = strconv.FormatInt(stats.Unique, 10)
			}
		}

		rows = append(rows, row)
	}

	return rows
}

// AggregateFile aggregates data from a file
// This convenience method demonstrates file-based aggregation.
func (s *AggregateService) AggregateFile(inputFile, outputFile string, rules []AggregateRule, groupBy []string) (*ProcessingResult, error) {
	s.logger.Info().
		Str("input", inputFile).
		Str("output", outputFile).
		Int("rules", len(rules)).
		Strs("group_by", groupBy).
		Msg("Starting file aggregation")

	options := map[string]interface{}{
		"input_file":  inputFile,
		"output_file": outputFile,
		"rules":       rules,
		"group_by":    groupBy,
	}

	resultData, err := s.ProcessData(nil, options)
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
		Processed:  len(resultData),
		OutputPath: outputFile,
		Processor:  s.GetName(),
	}, nil
}
