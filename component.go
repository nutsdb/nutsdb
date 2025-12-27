package nutsdb

import (
	"context"
	"time"
)

type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(timeout time.Duration) error
}
