package main

import (
	"os"
	"fmt"
	"flag"
	"encoding/json"
)

var (
	supperBlockFile string
)

func init() {
	flag.StringVar(&supperBlockFile, "i", "", "input a supper_block file")
}

func help() {
	fmt.Println("supper_block_parse -i supper_block_file")
}

func main() {
	var (
		err error
	)

	flag.Parse()

	if supperBlockFile == "" {
		help()
	}

	supperBlock := NewSuperBlock()

	supperBlock.fd, err = os.Open(supperBlockFile)
	if err != nil {
		return
	}

	err = supperBlock.doParse()
	res, err := json.Marshal(supperBlock)
	if err != nil {
		return
	}
	fmt.Println(string(res))

	supperBlock.fd.Close()
}