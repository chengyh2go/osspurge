package main

import (
	"fmt"
	"sort"
)

type dateList []string

func (list dateList) Len() int {
	return len(list)
}
func (list dateList) Less(i, j int) bool {
	return list[i] > list[j]
}
func (list dateList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func main() {

	arr1 := dateList{"20211012","20211013","20211011","20210923"}
	fmt.Println("排序前：",arr1)
	sort.Sort(arr1)

	fmt.Println("排序后",arr1)

	fmt.Println(arr1[3:])

}
