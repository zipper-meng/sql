package main

import (
	"fmt"
	"reflect"

	"sql/ast"
)

var _ ast.Visitor = &visitor{}

type visitor struct {
	n int
}

func (v *visitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}

	printSpace((v.n)*4 - 1)

	nodeVal := reflect.ValueOf(node)
	typeStr := nodeVal.Type().String()

	val := fmt.Sprintf("L%d: Type: %s, Value: %s", v.n, typeStr, node.String())
	fmt.Printf("%s\n", val)

	v.n = v.n + 1
	return v
}

// printFork print tab characters
// ├ ┤ │ ┐ ┘ ┌ └ ┼ ─ ┴ ┬
func (v *visitor) printFork() {
	// TODO
}

// printSpace print n space characters
func printSpace(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(" ")
	}
}

// printBranch print tab characters
// ├ ┤ │ ┐ ┘ ┌ └ ┼ ─ ┴ ┬
func printBranch(n int) {
	// TODO
}
