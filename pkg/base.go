package pkg

import (
	"github.com/samber/do-template-cli/pkg/cli"
	"github.com/samber/do-template-cli/pkg/config"
	"github.com/samber/do-template-cli/pkg/logger"
	"github.com/samber/do/v2"
)

var BasePackage = do.Package(
	do.Lazy(config.NewConfig),
	do.Lazy(cli.NewCLI),
	do.Lazy(logger.NewLogger),
)
