package cover

import (
	"fmt"
	"math/big"
	"ouroboros/src/node"
)

var S = "10000000000000000000000000000000000000000000000000000000000000000"
var Smin = "0000000000000000000000000000000000000000000000000000000000000000"
var Smax = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

var Imin = new(big.Int).SetUint64(uint64(0))
var Imax, _ = new(big.Int).SetString(Smax, 16)

//var I3 = new(big.Int).Sub(Imax, Imin)

//var bN = new(big.Int).SetUint64(uint64(N))
//var DI = new(big.Int).Div(I3, bN)

var Db0 = new(big.Int).Div(Imax, new(big.Int).SetUint64(100))
var Db1 = new(big.Int).Div(Imax, new(big.Int).SetUint64(1000))
var Db2 = new(big.Int).Div(Imax, new(big.Int).SetUint64(10000000))

var DI2 = new(big.Float).Quo(new(big.Float).SetInt(Imax), new(big.Float).SetInt64(10000000))

// A -is the by-layer data A[l][n] - is the node_id at layer l, position n

type Cover struct {
	nodes *map[string]node.Node
	A     [][]string
}

func (x *Cover) Strong() string {
	s := "cover\n"
	for l := range x.A {
		s += "l=" + fmt.Sprint(l) + " "
		for n := range x.A[l] {
			s += "n=" + fmt.Sprint(n) + " " + x.A[l][n] + " "
		}
		s += "\n\n"
	}
	return s
}

func fill(s string) string {
	if len(s) == 64 {
		return s
	}
	if len(s) > 64 {
		return Smax
	}
	m := 64 - len(s)
	s1 := make([]byte, m)
	for i := 0; i < m; i++ {
		s1[i] = 0x30
	}
	return string(s1) + s
}

func (x *Cover) SetPointer(nodes *map[string]node.Node) {
	x.nodes = nodes
}

func (x *Cover) CalculateDocRangeToNode(y node.Node) (string, string, bool, string, string) {

	l := len(x.A)
	disk := int64(y.D)
	id := y.Id
	var I12 string
	if l > 0 {
		l1 := len(x.A[l-1])
		if l1 > 0 {
			id1 := x.A[l-1][l1-1]
			I12 = (*x.nodes)[id1].I22
		} else {
			I12 = Smin
		}
	} else {
		I12 = Smin
	}

	if I12 == Smax {
		I12 = Smin
		x.A = append(x.A, make([]string, 0))
		l += 1
	}
	fmt.Println("I12==", I12)

	J12, _ := new(big.Int).SetString(I12, 16)

	fmt.Println(DI2, disk)

	var dI = new(big.Float).Mul(DI2, new(big.Float).SetFloat64(float64(disk)))

	fmt.Println("n=", disk, "y.D=", y.D, "dI", dI)

	var J22 = new(big.Float).Add(new(big.Float).SetInt(J12), dI)

	I22s := J22.Text('f', 0)

	I22p, _ := new(big.Int).SetString(I22s, 10)
	I22 := I22p.Text(16)
	I22 = fill(I22)

	fmt.Println("I22==", I22, "\n")

	if I22 <= Smax {
		x.A[l-1] = append(x.A[l-1], id)
		return I12, I22, false, "", ""
	} else {
		x.A[l-1] = append(x.A[l-1], id)
		x.A = append(x.A, make([]string, 0))

		I22r, _ := new(big.Int).SetString(I22, 16)

		var I22ss = new(big.Int).Sub(I22r, Imax)
		I22s := I22ss.Text(16)

		x.A[l] = append(x.A[l], id)
		return I12, Smax, true, Smin, I22s
	}
}

func CreateCover(nodes *map[string]node.Node) *Cover {
	var x Cover
	x.nodes = nodes
	x.A = make([][]string, 1)
	x.A[0] = make([]string, 0)

	for id := range *x.nodes {
		node := (*x.nodes)[id]
		//TODO : use uneven interval
		I12, I22, _, _, _ := x.CalculateDocRangeToNode(node)
		node.I12 = I12
		node.I22 = I22
		(*nodes)[id] = node
	}
	return &x
}
