package jobs

import "github.com/samber/do/v2"

var Package = do.Package(
	do.Lazy(NewFileService),
	do.Lazy(NewCSVToJSONService),
	do.Lazy(NewFilterService),
	do.Lazy(NewAggregateService),
	do.Lazy(NewValidateService),
	do.Lazy(NewTransformService),
)
