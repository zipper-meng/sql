package main

import (
	"fmt"
	"sql/parser"
)

func main() {
	stmt := "select * from ma"
	q, err := parser.ParseQuery(stmt)
	if err != nil {
		fmt.Printf("ERR: %s", err)
	} else {
		fmt.Println(q)
	}
}
