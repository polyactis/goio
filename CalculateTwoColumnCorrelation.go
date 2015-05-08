package goio

import (
	"fmt"
	"log"
	"math"
	"strconv"
)

type CalculateTwoColumnCorrelation struct {
	Mapper
	XColIndex int
	YColIndex int

	NumOfData         int64
	sum_x_squared     float64
	sum_x             float64
	sum_y_squared     float64
	sum_y             float64
	sum_x_multi_y     float64
	MeanX, MeanY, Cor float64
}

func (m *CalculateTwoColumnCorrelation) handleHeader(header *[]string) int {
	fmt.Println("XColIndex:", m.XColIndex)
	fmt.Println("YColIndex:", m.YColIndex)
	fmt.Println("Handling header ...")
	newHeader := []string{"#numberOfData|int", "correlationR2|float",
		"meanCol_" + strconv.FormatInt(int64(m.XColIndex), 10) + "_" + (*header)[m.XColIndex] + "|float",
		"meanCol_" + strconv.FormatInt(int64(m.YColIndex), 10) + "_" + (*header)[m.YColIndex] + "|float",
		"filename|str"}
	m.outputRowChannel <- newHeader
	return 1

}

func (m *CalculateTwoColumnCorrelation) handleDataRow(dataRow *[]string) int {
	if dataRow != nil {
		m.NumOfData = m.NumOfData + 1

		x, err := strconv.ParseFloat((*dataRow)[m.XColIndex], 64)
		if err != nil {
			log.Fatal("Error at line ", m.NumOfData, "data ", *dataRow, err)
		}
		y, err := strconv.ParseFloat((*dataRow)[m.YColIndex], 64)
		if err != nil {
			log.Fatal("Error at line ", m.NumOfData, "data ", *dataRow, err)
		}

		m.sum_x_squared = m.sum_x_squared + x*x
		m.sum_x = m.sum_x + x
		m.sum_y_squared = m.sum_y_squared + y*y
		m.sum_y = m.sum_y + y
		m.sum_x_multi_y = m.sum_x_multi_y + x*y

		return 1
	} else {
		return 0
	}

}

func (m *CalculateTwoColumnCorrelation) reduce() int {
	if m.NumOfData > 0 {
		numOfDataInFloat := float64(m.NumOfData)
		m.MeanX = m.sum_x / numOfDataInFloat
		m.MeanY = m.sum_y / numOfDataInFloat
		xy := m.sum_x_multi_y - m.MeanX*m.sum_y - m.MeanY*m.sum_x + numOfDataInFloat*m.MeanX*m.MeanY

		xx := m.sum_x_squared - 2*m.MeanX*m.sum_x + numOfDataInFloat*m.MeanX*m.MeanX
		yy := m.sum_y_squared - 2*m.MeanY*m.sum_y + numOfDataInFloat*m.MeanY*m.MeanY
		m.Cor = xy / (math.Sqrt(xx * yy))
	} else {
		m.MeanX = -1.0
		m.MeanY = -1.0
		m.Cor = -1.0
	}
	newDataRow := []string{strconv.FormatInt(m.NumOfData, 10),
		strconv.FormatFloat(m.Cor, 'f', 5, 64),
		strconv.FormatFloat(m.MeanX, 'f', 5, 64),
		strconv.FormatFloat(m.MeanY, 'f', 5, 64),
		m.InputFname}
	m.outputRowChannel <- newDataRow
	return 1
}
