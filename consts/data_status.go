package consts

type DataStatus int16

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted DataStatus = 0

	// Committed represents the tx committed status
	Committed DataStatus = 1
)

func (ds DataStatus) Is(s DataStatus) bool {
	return ds == s
}

func (ds DataStatus) Not(s DataStatus) bool {
	return ds != s
}

func (ds DataStatus) OneOf(status []DataStatus) bool {
	for _, s := range status {
		if ds == s {
			return true
		}
	}
	return false
}

func (ds DataStatus) NoneOf(status []DataStatus) bool {
	for _, s := range status {
		if ds == s {
			return false
		}
	}
	return true
}
