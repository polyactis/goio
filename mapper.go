package goio

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
)

type MapperInterface interface {
	PreProcess()
	PostProcess()
	Traverse()
	handleHeader(header *[]string) int
	handleDataRow(dataRow *[]string) int
	reduce() int
}

type Mapper struct {
	InputFname      string
	OutputFname     string
	IsHeaderPresent bool
	RowChannelSize  int

	inputRowChannel               chan []string
	outputRowChannel              chan []string
	header                        *[]string
	inputReader                   io.Reader
	gzipWriter                    *gzip.Writer
	noOfLines                     int64
	noOfLinesProcessed            int64
	noOfLinesOutputted            int64
	sumOfHandleDataRowReturnValue int64
	MInterface                    MapperInterface //to reach overridden methods
	wg                            sync.WaitGroup  //wait for all threads to finish before exiting
}

func NewMapper(InputFname string, OutputFname string, IsHeaderPresent bool, RowChannelSize int) *Mapper {

	m := new(Mapper)
	m.InputFname = InputFname
	m.OutputFname = OutputFname
	m.IsHeaderPresent = IsHeaderPresent
	m.RowChannelSize = RowChannelSize

	m.MInterface = m          //to reach overridden methods
	m.MInterface.PreProcess() //to reach overridden methods

	return m
}

func (m *Mapper) PreProcess() {
	m.noOfLines = 0
	m.noOfLinesProcessed = 0
	m.noOfLinesProcessed = 0
	if m.RowChannelSize == 0 {
		m.RowChannelSize = 10000
	}
	fmt.Println("IsHeaderPresent:", m.IsHeaderPresent)
	fmt.Println("RowChannelSize:", m.RowChannelSize)
	m.inputRowChannel = make(chan []string, m.RowChannelSize)
	m.outputRowChannel = make(chan []string, 100)

	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		fmt.Println("Started input-reading thread", m.InputFname, "...")
		m.inputReader = *OpenReader(m.InputFname)
		scanner := bufio.NewScanner(m.inputReader)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Split(line, "\t")
			if m.noOfLines == 0 {
				m.MInterface.handleHeader(&fields) //to reach overridden methods
			}
			if m.IsHeaderPresent {
				m.header = &fields
			}
			if m.noOfLines == 0 && m.IsHeaderPresent {

			} else {
				m.inputRowChannel <- fields
			}
			//inputRowChannel <- fields
			m.noOfLines++
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		close(m.inputRowChannel)
		fmt.Println("Input file", m.InputFname, "contains ", m.noOfLines, "lines.")
	}()

	go func() {
		defer m.wg.Done()
		fmt.Println("Started output thread", m.OutputFname, "...")
		m.gzipWriter = OpenGzipWriter(m.OutputFname)
		for dataRow := range m.outputRowChannel {
			m.noOfLinesOutputted++
			fmt.Fprintln(m.gzipWriter, strings.Join(dataRow, "\t"))
		}
		fmt.Println("Outputted", m.noOfLinesOutputted, "lines.")
		m.gzipWriter.Close()

	}()

}

func (m *Mapper) PostProcess() {
	close(m.outputRowChannel) //time to close the output channel

	fmt.Print("Waiting for threads to finish ... ")
	m.wg.Wait()
	fmt.Println("Done.")

}

// default is to copy the input header if it's present
func (m *Mapper) handleHeader(header *[]string) int {

	if header != nil && m.IsHeaderPresent {
		m.header = header
		fmt.Println("Handling header ...")
		m.outputRowChannel <- *header
		//fmt.Fprintln(m.gzipWriter, strings.Join(*header, "\t"))
		return 1
	} else {
		return 0
	}

}

func (m *Mapper) handleDataRow(dataRow *[]string) int {
	if dataRow != nil {
		m.outputRowChannel <- *dataRow
		return 1
	} else {
		return 0
	}

}
func (m *Mapper) reduce() int {
	return 0

}

func (m *Mapper) Traverse() {
	fmt.Println("Start processing input data  ...")
	for dataRow := range m.inputRowChannel {
		m.noOfLinesProcessed++
		//fmt.Println(strings.Join(dataRow, "\t"))
		m.sumOfHandleDataRowReturnValue = m.sumOfHandleDataRowReturnValue + int64(m.MInterface.handleDataRow(&dataRow))
	}
	m.MInterface.reduce()

	fmt.Println("Processed", m.noOfLinesProcessed, "lines. sumOfHandleDataRowReturnValue:", m.sumOfHandleDataRowReturnValue, ".")
}
