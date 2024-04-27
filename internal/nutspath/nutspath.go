package nutspath

import "path/filepath"

type Path string

func getPath(path string, operations ...func(string) string) Path {
	for _, op := range operations {
		path = op(path)
	}

	return Path(filepath.Clean(path))
}

func New(path string) Path {
	return getPath(path)
}

func (p Path) String() string {
	return string(p)
}

func (p Path) Join(elem ...string) Path {
	return getPath(p.String(), func(s string) string {
		return filepath.Join(append([]string{s}, elem...)...)
	})
}
