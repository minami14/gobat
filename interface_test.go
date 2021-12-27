//go:generate mockgen -source=$GOFILE -destination=mock_$GOFILE -package=$GOPACKAGE

package gobat_test

import "context"

type JobHandlerInterface interface {
	Handler(context.Context, int, error) error
}
