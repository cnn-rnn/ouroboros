package return_tree

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"time"

	"ouroboros/src/constants"
	"ouroboros/src/node"
)

/*
var S = "10000000000000000000000000000000000000000000000000000000000000000"
var Smin = "0000000000000000000000000000000000000000000000000000000000000000"
var Smax = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

var Imin = new(big.Int).SetUint64(uint64(0))
var Imax, _ = new(big.Int).SetString(Smax, 16)
*/

type Interv struct {
	I1 string
	I2 string
}

type Roles struct { //  R[l] = array of Interv that this node serves
	Id string
	R  map[int][]Interv
}

type ReturnTree struct {
	NodeIds []string

	D     int // the degree
	Dobro int // dobrotnost'
	L     int // number of levels; last level - is the level of Gateways

	LayerMap [][][]string       // [l][n][i] l -layer, n - group , i - number
	NodeMap  map[string][][]int //   [id][layer][group]
	RoleMap  map[string]Roles   // node -> roles

	ra    *rand.Rand
	nodes *map[string]node.Node
}

func (x *ReturnTree) Strong() string {
	if x.nodes == nil {
		return "x.nodes==nil"
	}
	sl := make([]string, 0)
	for id := range *x.nodes {
		sl = append(sl, id)
	}
	sort.Strings(sl)

	s := "layerMap:\n"
	for l := range x.LayerMap {
		s += "l=" + fmt.Sprint(l) + "\n"
		for n := range x.LayerMap[l] {
			s += "n=" + fmt.Sprint(n) + " " + fmt.Sprintln(x.LayerMap[l][n])
		}
	}
	s += "\nNodeMap:\n"
	for i := range sl {
		id := sl[i]
		s += "id=" + id + "\n"
		for l := range x.NodeMap[id] {
			s += "l=" + fmt.Sprint(l) + " : " + fmt.Sprintln(x.NodeMap[id][l]) + "\n"
		}
	}
	s += "\nRoleMap:\n"
	for i := range sl {
		id := sl[i]
		s += "id " + id + "\n"
		for l := range x.RoleMap[id].R {
			s += "l=" + fmt.Sprint(l) + " " + fmt.Sprintln(x.RoleMap[id].R[l])
		}
	}
	return s
}

func (x *ReturnTree) eligible(l int, n int) []string {
	m1 := 100
	m2 := -1
	for id := range *x.nodes {
		if (*x.nodes)[id].Status != constants.Active {
			continue
		}
		d := len(x.NodeMap[id][l])
		if d < m1 {
			m1 = d
		}
		if d > m2 {
			m2 = d
		}
	}
	if m2-m1 > 1 {
		log.Fatal("m1", m1, "m2", m2)
	}
	y := make([]string, 0)
	for id := range *x.nodes {
		if (*x.nodes)[id].Status != constants.Active {
			continue
		}
		B := true
		for j := range x.NodeMap[id][l] {
			if x.NodeMap[id][l][j] == n {
				B = false
			}
		}
		if B && len(x.NodeMap[id][l]) == m1 {
			y = append(y, id)
		}
	}
	if len(y) == 0 {
		for id := range *x.nodes {
			if (*x.nodes)[id].Status != constants.Active {
				continue
			}
			y = append(y, id)
		}
	}
	return y
}

func (x *ReturnTree) draw(l int, n int) string {
	y := x.eligible(l, n)
	if len(y) == 0 {
		return ""
	}
	k := x.ra.Intn(len(y))
	return y[k]
}

func (x *ReturnTree) createLayer(l int) bool {
	N := int(math.Pow(float64(x.D), float64(l)))

	active := 0
	for id := range *x.nodes {
		if (*x.nodes)[id].Status == constants.Active {
			active += 1
		}
	}

	log.Println("active=", active)

	d0 := x.Dobro
	d1 := active / N
	if d1 > d0 {
		d0 = d1
	}
	x.LayerMap[l] = make([][]string, N)

	m := 0
	for id := range *x.nodes {
		log.Println("createLayer node status", (*x.nodes)[id].Status)
		if (*x.nodes)[id].Status != constants.Active {
			continue
		}
		m += 1
		x.NodeMap[id][l] = make([]int, 0)
	}
	log.Println("m=", m)
	for n := 0; n < N; n++ {
		x.LayerMap[l][n] = make([]string, 0)
		for i := 0; i < d0; i++ {
			id := x.draw(l, n)

			fmt.Println("   id", id)

			if id == "" {
				return false
			}
			x.LayerMap[l][n] = append(x.LayerMap[l][n], id)
			x.NodeMap[id][l] = append(x.NodeMap[id][l], n)
		}
	}
	return true
}

func (x *ReturnTree) SetPointer(nodes *map[string]node.Node) {
	x.nodes = nodes
}

func CreateRTree(nodes *map[string]node.Node, L int, D int, dobro int) (ReturnTree, error) {

	var x ReturnTree
	x.L = L
	x.D = D
	x.Dobro = dobro
	x.nodes = nodes

	var rs = rand.NewSource(time.Now().UnixNano())
	var ra = rand.New(rs)

	x.ra = ra

	x.NodeMap = make(map[string][][]int)
	x.LayerMap = make([][][]string, L)
	for id := range *x.nodes {
		if (*x.nodes)[id].Status != constants.Active {
			continue
		}
		x.NodeMap[id] = make([][]int, L)
	}
	for l := 0; l < L; l++ {
		b := x.createLayer(l)
		if !b {
			return ReturnTree{}, errors.New("no nodes available")
		}
		log.Println("laeMap", x.LayerMap, "\n")
		for n := range x.LayerMap[l] {
			fmt.Println("layer", l, n, x.LayerMap[l][n])
		}
	}

	x.RoleMap = make(map[string]Roles)
	for id := range *x.nodes {
		if (*x.nodes)[id].Status != constants.Active {
			continue
		}
		var r Roles
		r.R = make(map[int][]Interv)
		for l := 0; l < L; l++ {
			N := int64(math.Pow(float64(D), float64(l)))
			di := new(big.Int).Div(constants.Imax, new(big.Int).SetInt64(N))
			for i := range x.NodeMap[id][l] {
				n := int64(x.NodeMap[id][l][i]) // [n,n+1]
				i1 := new(big.Int).Mul(di, new(big.Int).SetInt64(n))
				i2 := new(big.Int).Mul(di, new(big.Int).SetInt64(n+1))
				p1 := fmt.Sprintf("%x", i1)
				p2 := fmt.Sprintf("%x", i2)
				q := Interv{I1: p1, I2: p2}
				r.R[l] = append(r.R[l], q)
			}
		}
		r.Id = id
		x.RoleMap[id] = r
	}

	return x, nil
}
