package nutsdb

import "github.com/nutsdb/nutsdb/internal/core"

// Public errors. These are re-exports of the definitions in internal/core so
// that they are part of the nutsdb public API while keeping a single error
// identity (errors.Is works across nutsdb.ErrXxx and core.ErrXxx).
//
// They are defined as aliases rather than moved out of internal/core to avoid
// an import cycle (internal/core is imported by this package).
var (
	// ErrCrc is returned when a CRC check fails while reading an entry.
	ErrCrc = core.ErrCrc
	// ErrCapacity is returned when an invalid capacity is provided.
	ErrCapacity  = core.ErrCapacity
	ErrEntryZero = core.ErrEntryZero
)
