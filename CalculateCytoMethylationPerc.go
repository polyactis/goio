package goio

import (
	"fmt"
	"log"
	"strconv"
)

type CalulateCytoMethylationPerc struct {
	Mapper
	MinCov      int
	CytoContext string
}

func (m *CalulateCytoMethylationPerc) handleDataRow(dataRow *[]string) int {
	if dataRow != nil {
		locusCytoContext := (*dataRow)[5]
		if m.CytoContext != "" && m.CytoContext != locusCytoContext {
			return 0
		}
		methylCountStr := (*dataRow)[3]
		methylCount, err := strconv.ParseInt(methylCountStr, 10, 32)
		if err != nil {
			log.Fatal(err)
		}

		unMethylCount, err := strconv.ParseInt((*dataRow)[4], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		var coverage int64 = methylCount + unMethylCount
		if coverage < int64(m.MinCov) {
			return 0
		}
		startPosStr := (*dataRow)[1]
		startPos, err := strconv.ParseInt(startPosStr, 10, 32)
		zeroBasedStartPos := startPos - 1
		if err != nil {
			log.Fatal(err)
		}
		strand := (*dataRow)[2]
		var methylFraction float64
		if coverage > 0 {
			methylFraction = float64(methylCount) / float64(methylCount+unMethylCount)
		} else {
			methylFraction = -1.0
		}

		newDataRow := []string{(*dataRow)[0], strconv.FormatInt(zeroBasedStartPos, 10),
			startPosStr,
			strand,
			methylCountStr,
			strconv.FormatInt(coverage, 10),
			strconv.FormatFloat(methylFraction, 'f', 5, 64),
			locusCytoContext,
			(*dataRow)[6]}
		//*dataRow = append(*dataRow, strconv.FormatInt(coverage, 10), strconv.FormatFloat(methylFraction, 'f', 5, 64))
		m.outputRowChannel <- newDataRow
		//fmt.Fprintln(m.gzipWriter, strings.Join(newDataRow, "\t"))
		return 1
	} else {
		return 0
	}

}

func (m *CalulateCytoMethylationPerc) handleHeader(header *[]string) int {
	fmt.Println("Handling header ...")
	//fmt.Fprintln(m.gzipWriter, strings.Join(*header, "\t"))
	newHeader := []string{"#Chromosome", "0BasedStart", "Stop",
		"Strand", "MethylCount", "TotalCount", "MethylFraction",
		"CytoContext", "CytoTriNucleotide"}
	m.outputRowChannel <- newHeader
	//fmt.Fprintln(m.gzipWriter, strings.Join(newHeader, "\t"))
	return 1

}
