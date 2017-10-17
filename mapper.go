package goio

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const bufferSize = 32 * 1024 * 1024

type handleHeaderFuncType func(*[]string) int
type handleDataFuncType func(*[]string) int
type reduceFuncType func() int

type MapperInterface interface {
	PreProcess(handleHeaderFn handleHeaderFuncType)
	PostProcess()
	Traverse(handleDataFn handleDataFuncType, reduceFn reduceFuncType)
	HandleHeader(header *[]string) int
	HandleDataRow(dataRow *[]string) int
	Reduce() int
}

type Mapper struct {
	MInterface MapperInterface //to reach overridden methods

	InputFname      string
	OutputFname     string
	IsHeaderPresent bool
	RowChannelSize  int
	Delimiter       string

	InputRowChannel               chan []string
	OutputRowChannel              chan []string
	Header                        *[]string
	inputReader                   io.Reader
	gzipWriter                    *gzip.Writer
	noOfLines                     int64
	noOfLinesProcessed            int64
	noOfLinesOutputted            int64
	sumOfHandleDataRowReturnValue int64

	WaitComputing    sync.WaitGroup //wait for all threads to finish before exiting
	WaitOutputThread sync.WaitGroup
}

func NewMapper(InputFname string, OutputFname string, IsHeaderPresent bool, RowChannelSize int) *Mapper {

	m := new(Mapper)
	m.InputFname = InputFname
	m.OutputFname = OutputFname
	m.IsHeaderPresent = IsHeaderPresent
	m.RowChannelSize = RowChannelSize
	m.Delimiter = "\t"

	m.MInterface = m //to reach overridden methods
	return m
}

func (m *Mapper) PreProcess(handleHeaderFn handleHeaderFuncType) {
	m.noOfLines = 0
	m.noOfLinesProcessed = 0
	m.noOfLinesProcessed = 0
	if m.RowChannelSize == 0 {
		m.RowChannelSize = 10000
	}
	if m.Delimiter == "" {
		m.Delimiter = "\t"
	}
	fmt.Println("IsHeaderPresent:", m.IsHeaderPresent, ".")
	fmt.Println("RowChannelSize:", m.RowChannelSize, ".")
	fmt.Println("Delimiter:", m.Delimiter, ".")
	m.InputRowChannel = make(chan []string, m.RowChannelSize)
	m.OutputRowChannel = make(chan []string, 100)

	m.WaitComputing.Add(1)
	go func() {
		defer m.WaitComputing.Done()
		fmt.Println("Started input-reading thread", m.InputFname, "...")
		m.inputReader = OpenReader(m.InputFname)
		r := bufio.NewReaderSize(m.inputReader, bufferSize)
		byteLine, isPrefix, err := r.ReadLine()
		for err == nil && !isPrefix {
			line := string(byteLine)
			//fmt.Println(line)
			fields := strings.Split(line, m.Delimiter)
			if m.noOfLines == 0 {
				//m.MInterface.HandleHeader(&fields) //to reach overridden methods
				handleHeaderFn(&fields)
			}
			if m.IsHeaderPresent {
				m.Header = &fields
			}
			if m.noOfLines == 0 && m.IsHeaderPresent {

			} else {
				m.InputRowChannel <- fields
			}
			//inputRowChannel <- fields
			m.noOfLines++
			byteLine, isPrefix, err = r.ReadLine()
		}
		if isPrefix {
			fmt.Println("Error: buffer size", bufferSize, "is too small for this input file.")
			os.Exit(2)
		}
		if err != io.EOF {
			log.Fatal(err)
		}
		/*
			//not good when one line is too long, buffer size for scanner is limited.
				scanner := bufio.NewScanner(m.inputReader)
				for scanner.Scan() {
					line := scanner.Text()
					fields := strings.Split(line, m.Delimiter)
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
		*/
		close(m.InputRowChannel)
		fmt.Println("Input file", m.InputFname, "contains ", m.noOfLines, "lines.")
	}()

	m.WaitOutputThread.Add(1)
	go func() {
		defer m.WaitOutputThread.Done()
		fmt.Println("Started output thread", m.OutputFname, "...")
		m.gzipWriter = OpenGzipWriter(m.OutputFname)
		for dataRow := range m.OutputRowChannel {
			m.noOfLinesOutputted++
			fmt.Fprintln(m.gzipWriter, strings.Join(dataRow, "\t"))
		}
		fmt.Println("Outputted", m.noOfLinesOutputted, "lines.")
		m.gzipWriter.Close()

	}()

}

func (m *Mapper) PostProcess() {
	fmt.Print("Waiting for all computing threads to finish ... ")
	m.WaitComputing.Wait()
	fmt.Println("Done.")

	// all computing is over. time to close the output channel.
	close(m.OutputRowChannel)
	fmt.Print("Waiting for the output thread to finish ... ")
	m.WaitOutputThread.Wait()
	fmt.Println("Done.")

}

// default is to copy the input header if it's present
func (m *Mapper) HandleHeader(header *[]string) int {

	if header != nil && m.IsHeaderPresent {
		m.Header = header
		fmt.Println("Handling header ...")
		m.OutputRowChannel <- *header
		//fmt.Fprintln(m.gzipWriter, strings.Join(*header, "\t"))
		return 1
	} else {
		return 0
	}

}

func (m *Mapper) HandleDataRow(dataRow *[]string) int {
	if dataRow != nil {
		m.OutputRowChannel <- *dataRow
		return 1
	} else {
		return 0
	}

}
func (m *Mapper) Reduce() int {
	return 0

}

func (m *Mapper) Traverse(handleDataFn handleDataFuncType, reduceFn reduceFuncType) {
	fmt.Println("Start processing input data  ...")
	for dataRow := range m.InputRowChannel {
		m.noOfLinesProcessed++
		//fmt.Println(strings.Join(dataRow, "\t"))
		m.sumOfHandleDataRowReturnValue = m.sumOfHandleDataRowReturnValue + int64(handleDataFn(&dataRow))
	}
	reduceFn()
	//m.MInterface.Reduce()

	fmt.Println("Processed", m.noOfLinesProcessed, "lines. sumOfHandleDataRowReturnValue:", m.sumOfHandleDataRowReturnValue, ".")
}
