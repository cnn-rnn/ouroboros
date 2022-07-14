package load_conf

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"ouroboros/src/matri"
	"ouroboros/src/utils"
)

func LoadWaitA(fname string) ([][]int, [][][]int) {
BEG:
	r, e := ioutil.ReadFile(fname)
	if e != nil {
		log.Printf("waiting for A conf")
		time.Sleep(60 * time.Second)
		goto BEG
	}
	var X [][]int
	e = json.Unmarshal(r, &X)
	utils.Check(e, "wrong matrix format")
	X1 := X
	var Y [][][]int
	Y = append(Y, X)
	for t := 1; t < 4; t++ {
		X1 = matri.Times(X1, X)
		Y = append(Y, X1)
	}
	return X, Y
}

func LoadWaitInterface(fname string, x interface{}) {
BEG:
	r, e := ioutil.ReadFile(fname)
	if e != nil {
		log.Println("waiting for file", fname)
		time.Sleep(60 * time.Second)
		goto BEG
	}
	e = json.Unmarshal(r, &x)
	utils.Check(e, "wrong peer data format")
}
