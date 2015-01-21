package goio

import (
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func Reader(f *os.File) (io.Reader, error) {
	switch filepath.Ext(f.Name()) {
	case ".gz":
		fmt.Println("Use gzip NewReader")
		return gzip.NewReader(f)
	case ".bz2":
		return bzip2.NewReader(f), nil
	case ".zip":
		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}
		size := fi.Size()
		r, err := zip.NewReader(f, size)
		if err != nil {
			return nil, err
		}
		var files []io.Reader
		for _, file := range r.File {
			f, err := file.Open()
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		return io.MultiReader(files...), nil
	default:
		return f, nil
	}
}
func Writer(f *os.File) (io.Writer, error) {
	switch filepath.Ext(f.Name()) {
	case ".gz":
		fmt.Println("Use gzip NewWriter")
		return gzip.NewWriter(f), nil
	default:
		return f, nil
	}
}
