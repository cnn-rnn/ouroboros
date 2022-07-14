package routing

import (
	"errors"
	"log"
	"ouroboros/src/constants"
	"ouroboros/src/matri"
	"ouroboros/src/node"
	"strconv"
	"time"
)

type Routing struct {
	nodes *map[string]node.Node

	RM  [][]int
	Inv []string // tthis maps n -> id, where n is the routing role ;   Nodes.N already contain role N assignment
}

func (x *Routing) SetPointer(nodes *map[string]node.Node) {
	x.nodes = nodes
}

func (x *Routing) assignRoutingRoles() {
	n := 0
	active := 0
	for i := range *x.nodes {
		if (*x.nodes)[i].Status == constants.Active {
			active += 1
		}
	}

	log.Println("active", active)

	x.Inv = make([]string, active)
	for i := range *x.nodes {
		node := (*x.nodes)[i]
		if node.Status != constants.Active {
			continue
		}
		node.N = n
		(*x.nodes)[i] = node
		x.Inv[n] = i
		n += 1
	}
}

func CreateRoutingMatrix(nodes *map[string]node.Node, streams int) (Routing, error) {

	var x Routing
	x.nodes = nodes
	active := 0

	for i := range *x.nodes {
		if (*x.nodes)[i].Status == constants.Active {
			active += 1
		}
	}

	log.Println("active", active)

	if active < 4 {
		return Routing{}, errors.New("cluster too small, cannot route")
	}

	var d1 int
	if streams < active/2 {
		d1 = streams
	} else {
		d1 = active / 2
	}
BEG:

	X1 := matri.RandMat(d1, active)
	var Z = X1
	Y1 := make([][][]int, 5)
	Y1[0] = X1
	for t := 1; t < 5; t++ {
		Z = matri.Times(X1, Z)
		Y1[t] = Z
	}
	for i := range Y1[4] {
		for j := range Y1[4][i] {
			if i != j && Y1[4][i][j] == 0 {
				log.Println("invalid routing matrix generated", Y1[4])
				time.Sleep(time.Second)
				goto BEG
			}
		}
	}
	x.RM = X1
	x.assignRoutingRoles()
	return x, nil
}

func (x *Routing) Strong() string {
	if x.nodes == nil {
		return "Routing: no nodes available"
	}
	s := "RM:\n"
	for i := range x.RM {
		for j := range x.RM[i] {
			s += strconv.Itoa(x.RM[i][j]) + " "
		}
		s += "\n"
	}
	s += "Inv:\n"
	for i := range x.Inv {
		s += strconv.Itoa(i) + " : " + x.Inv[i] + "\n"
	}
	return s
}
