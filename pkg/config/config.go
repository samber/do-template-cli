package config

import (
	"fmt"
	"strings"

	"github.com/samber/do/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config holds all application configuration
// This struct demonstrates how to structure configuration for dependency injection.
type Config struct {
	Logger LoggerConfig `mapstructure:"logger"`
	App    AppConfig    `mapstructure:"app"`
}

// LoggerConfig holds logger configuration.
type LoggerConfig struct {
	Level   string `mapstructure:"level"`
	Format  string `mapstructure:"format"`
	Output  string `mapstructure:"output"`
	NoColor bool   `mapstructure:"no_color"`
}

// AppConfig holds application-specific configuration.
type AppConfig struct {
	Name        string `mapstructure:"name"`
	Version     string `mapstructure:"version"`
	Environment string `mapstructure:"environment"`
	Debug       bool   `mapstructure:"debug"`
}

// NewConfig creates a new configuration instance using viper
// This demonstrates configuration management with the samber/do library.
func NewConfig(i do.Injector) (*Config, error) {
	// Enable environment variable support
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("_", "."))

	// Unmarshal configuration into struct
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &config, nil
}

// SetCobraFlags adds command line flags to the cobra command
// This method demonstrates how services can provide functionality through DI.
func (cs *Config) SetCobraFlags(cmd *cobra.Command) {
	// Logger flags
	_ = cmd.PersistentFlags().String("logger.level", "info", "Log level")
	_ = cmd.PersistentFlags().String("logger.format", "console", "Log format")
	_ = cmd.PersistentFlags().String("logger.output", "stdout", "Log output")
	_ = cmd.PersistentFlags().Bool("logger.no_color", false, "Disable colored output")

	// App flags
	_ = cmd.PersistentFlags().String("app.name", "do-template-cli", "Application name")
	_ = cmd.PersistentFlags().String("app.version", "1.0.0", "Application version")
	_ = cmd.PersistentFlags().String("app.environment", "development", "Application environment")
	_ = cmd.PersistentFlags().Bool("app.debug", false, "Debug mode")

	// Bind all flags to viper for automatic configuration
	cs.bindFlagsToViper(cmd)
}

// bindFlagsToViper binds all cobra flags to viper.
func (cs *Config) bindFlagsToViper(cmd *cobra.Command) {
	// Logger flags
	_ = viper.BindPFlag("logger.level", cmd.PersistentFlags().Lookup("logger.level"))
	_ = viper.BindPFlag("logger.format", cmd.PersistentFlags().Lookup("logger.format"))
	_ = viper.BindPFlag("logger.output", cmd.PersistentFlags().Lookup("logger.output"))
	_ = viper.BindPFlag("logger.no_color", cmd.PersistentFlags().Lookup("logger.no_color"))

	// App flags
	_ = viper.BindPFlag("app.name", cmd.PersistentFlags().Lookup("app.name"))
	_ = viper.BindPFlag("app.version", cmd.PersistentFlags().Lookup("app.version"))
	_ = viper.BindPFlag("app.environment", cmd.PersistentFlags().Lookup("app.environment"))
	_ = viper.BindPFlag("app.debug", cmd.PersistentFlags().Lookup("app.debug"))
}
