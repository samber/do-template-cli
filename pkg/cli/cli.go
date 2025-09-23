package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/samber/do-template-cli/pkg/config"
	"github.com/samber/do-template-cli/pkg/jobs"
	"github.com/samber/do/v2"
	"github.com/spf13/cobra"
)

// CLI represents the command line interface service
// This demonstrates how to create a CLI service with dependency injection.
type CLI struct {
	config      *config.Config `do:""`
	injector    do.Injector
	rootCommand *cobra.Command
}

// NewCLI creates a new CLI service with dependency injection support.
func NewCLI(i do.Injector) (*CLI, error) {
	cli := do.MustInvokeStruct[*CLI](i)
	cli.injector = i

	// Create the root command
	cli.rootCommand = &cobra.Command{
		Use:     cli.config.App.Name,
		Short:   "A template cli application using samber/do dependency injection",
		Long:    "A comprehensive template project demonstrating the github.com/samber/do dependency injection library with PostgreSQL and RabbitMQ integration",
		Version: cli.config.App.Version,
	}

	// Add persistent flags using dependency injection
	cli.setupPersistentFlags()

	// Add commands
	cli.setupCommands()

	return cli, nil
}

// setupPersistentFlags adds global flags to the CLI.
func (cli *CLI) setupPersistentFlags() {
	// Use the config service to set up all configuration flags
	// This demonstrates dependency injection for configuration management
	cli.config.SetCobraFlags(cli.rootCommand)
}

// setupCommands adds subcommands to the CLI.
func (cli *CLI) setupCommands() {
	// Add serve command
	cli.rootCommand.AddCommand(cli.newServeCommand())

	// Add migrate command
	cli.rootCommand.AddCommand(cli.newMigrateCommand())

	// Add health command
	cli.rootCommand.AddCommand(cli.newHealthCommand())

	// Add version command
	cli.rootCommand.AddCommand(cli.newVersionCommand())

	// Add data processing commands
	cli.rootCommand.AddCommand(cli.newCSVToJSONCommand())
	cli.rootCommand.AddCommand(cli.newFilterCommand())
	cli.rootCommand.AddCommand(cli.newAggregateCommand())
	cli.rootCommand.AddCommand(cli.newValidateCommand())
	cli.rootCommand.AddCommand(cli.newTransformCommand())
}

// newServeCommand creates the serve command.
func (cli *CLI) newServeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the cli service",
		Long:  "Start the do-template-cli service with dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Starting cli service...")
			// This will be implemented to use the dependency injection container
		},
	}
}

// newMigrateCommand creates the migrate command.
func (cli *CLI) newMigrateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Run database migrations",
		Long:  "Run database migrations using the configured database connection",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Running database migrations...")
			// This will be implemented to use the dependency injection container
		},
	}
}

// newHealthCommand creates the health command.
func (cli *CLI) newHealthCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check service health",
		Long:  "Check the health of all services and dependencies",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Checking service health...")
			// This will be implemented to use the dependency injection container
		},
	}
}

// newVersionCommand creates the version command.
func (cli *CLI) newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  "Show detailed version and build information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("%s version %s\n", cli.config.App.Name, cli.config.App.Version)
		},
	}
}

// RootCommand returns the root cobra command.
func (cli *CLI) RootCommand() *cobra.Command {
	return cli.rootCommand
}

// Execute executes the CLI with the given arguments.
func (cli *CLI) Execute() error {
	return cli.rootCommand.Execute()
}

// newCSVToJSONCommand creates the CSV to JSON conversion command.
func (cli *CLI) newCSVToJSONCommand() *cobra.Command {
	var inputFile, outputFile string

	cmd := &cobra.Command{
		Use:   "csv-to-json",
		Short: "Convert CSV files to JSON format",
		Long:  "Convert CSV files to JSON format using dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" {
				fmt.Println("Error: input file is required")
				os.Exit(1)
			}

			// Get the CSV to JSON service from dependency injection container
			service := do.MustInvoke[*jobs.CSVToJSONService](cli.injector)

			result, err := service.ConvertFile(inputFile, outputFile)
			if err != nil {
				fmt.Printf("Error converting CSV to JSON: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Successfully converted %d records from %s to %s\n",
				result.Processed, inputFile, result.OutputPath)
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file (optional)")

	return cmd
}

// newFilterCommand creates the data filtering command.
func (cli *CLI) newFilterCommand() *cobra.Command {
	var inputFile, outputFile string
	var rulesJSON string
	var inclusive bool

	cmd := &cobra.Command{
		Use:   "filter-data",
		Short: "Filter data based on field conditions",
		Long:  "Filter data based on field conditions using dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" || rulesJSON == "" {
				fmt.Println("Error: input file and rules are required")
				os.Exit(1)
			}

			// Parse filter rules from JSON
			var rules []jobs.FilterRule
			if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil {
				fmt.Printf("Error parsing filter rules: %v\n", err)
				os.Exit(1)
			}

			// Get the filter service from dependency injection container
			service := do.MustInvoke[*jobs.FilterService](cli.injector)

			result, err := service.FilterByFile(inputFile, outputFile, rules, inclusive)
			if err != nil {
				fmt.Printf("Error filtering data: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Successfully filtered %d records from %s to %s\n",
				result.Processed, inputFile, result.OutputPath)
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file (optional)")
	cmd.Flags().StringVar(&rulesJSON, "rules", "", "Filter rules in JSON format (required)")
	cmd.Flags().BoolVar(&inclusive, "inclusive", true, "Include matching records (true) or exclude them (false)")

	return cmd
}

// newAggregateCommand creates the data aggregation command.
func (cli *CLI) newAggregateCommand() *cobra.Command {
	var inputFile, outputFile string
	var rulesJSON, groupByJSON string

	cmd := &cobra.Command{
		Use:   "aggregate-data",
		Short: "Aggregate and summarize data with statistical operations",
		Long:  "Aggregate and summarize data with statistical operations using dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" || rulesJSON == "" {
				fmt.Println("Error: input file and rules are required")
				os.Exit(1)
			}

			// Parse aggregation rules from JSON
			var rules []jobs.AggregateRule
			if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil {
				fmt.Printf("Error parsing aggregation rules: %v\n", err)
				os.Exit(1)
			}

			// Parse group by fields from JSON
			var groupBy []string
			if groupByJSON != "" {
				if err := json.Unmarshal([]byte(groupByJSON), &groupBy); err != nil {
					fmt.Printf("Error parsing group by fields: %v\n", err)
					os.Exit(1)
				}
			}

			// Get the aggregate service from dependency injection container
			service := do.MustInvoke[*jobs.AggregateService](cli.injector)

			result, err := service.AggregateFile(inputFile, outputFile, rules, groupBy)
			if err != nil {
				fmt.Printf("Error aggregating data: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Successfully aggregated %d records from %s to %s\n",
				result.Processed, inputFile, result.OutputPath)
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file (optional)")
	cmd.Flags().StringVar(&rulesJSON, "rules", "", "Aggregation rules in JSON format (required)")
	cmd.Flags().StringVar(&groupByJSON, "group-by", "", "Group by fields in JSON format (optional)")

	return cmd
}

// newValidateCommand creates the data validation command.
func (cli *CLI) newValidateCommand() *cobra.Command {
	var inputFile, outputFile string
	var rulesJSON string
	var failFast bool

	cmd := &cobra.Command{
		Use:   "validate-data",
		Short: "Validate data integrity and quality",
		Long:  "Validate data integrity and quality using dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" || rulesJSON == "" {
				fmt.Println("Error: input file and rules are required")
				os.Exit(1)
			}

			// Parse validation rules from JSON
			var rules []jobs.ValidationRule
			if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil {
				fmt.Printf("Error parsing validation rules: %v\n", err)
				os.Exit(1)
			}

			// Get the validate service from dependency injection container
			service := do.MustInvoke[*jobs.ValidateService](cli.injector)

			result, err := service.ValidateFile(inputFile, outputFile, rules, failFast)
			if err != nil {
				fmt.Printf("Error validating data: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Data validation completed:\n")
			fmt.Printf("  Total records: %d\n", result.TotalRows)
			fmt.Printf("  Valid records: %d\n", result.ValidRows)
			fmt.Printf("  Invalid records: %d\n", result.InvalidRows)
			fmt.Printf("  Quality score: %.2f%%\n", result.QualityScore)
			fmt.Printf("  Output saved to: %s\n", outputFile)
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file (optional)")
	cmd.Flags().StringVar(&rulesJSON, "rules", "", "Validation rules in JSON format (required)")
	cmd.Flags().BoolVar(&failFast, "fail-fast", false, "Stop validation on first error")

	return cmd
}

// newTransformCommand creates the data transformation command.
func (cli *CLI) newTransformCommand() *cobra.Command {
	var inputFile, outputFile string
	var rulesJSON string
	var keepFields bool

	cmd := &cobra.Command{
		Use:   "transform-data",
		Short: "Transform data fields with various operations",
		Long:  "Transform data fields with various operations using dependency injection",
		Run: func(cmd *cobra.Command, args []string) {
			if inputFile == "" || rulesJSON == "" {
				fmt.Println("Error: input file and rules are required")
				os.Exit(1)
			}

			// Parse transformation rules from JSON
			var rules []jobs.TransformRule
			if err := json.Unmarshal([]byte(rulesJSON), &rules); err != nil {
				fmt.Printf("Error parsing transformation rules: %v\n", err)
				os.Exit(1)
			}

			// Get the transform service from dependency injection container
			service := do.MustInvoke[*jobs.TransformService](cli.injector)

			result, err := service.TransformFile(inputFile, outputFile, rules, keepFields)
			if err != nil {
				fmt.Printf("Error transforming data: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Successfully transformed %d records from %s to %s\n",
				result.Processed, inputFile, result.OutputPath)
		},
	}

	cmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file (required)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output JSON file (optional)")
	cmd.Flags().StringVar(&rulesJSON, "rules", "", "Transformation rules in JSON format (required)")
	cmd.Flags().BoolVar(&keepFields, "keep-fields", true, "Keep non-transformed fields")

	return cmd
}

// AddCommand adds a new command to the CLI.
func (cli *CLI) AddCommand(command *cobra.Command) {
	cli.rootCommand.AddCommand(command)
}
