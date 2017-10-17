package goio

import (
	"github.com/gonum/matrix/mat64"
)

type DataFrame struct {
	Filename   string
	RowIDArray []string
	ColIDArray []string
	Delimiter  string
	DataMatrix mat64.Dense

	header []string

	turn_into_integer  int
	ignore_2nd_column  int
	turn_into_array    bool
	matrix_data_type   int
	matrix_orientation int
	data_starting_col  int
}
