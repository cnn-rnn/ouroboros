package btr

import (
	"errors"
	"log"
	"math/rand"
	"os"
)

type Tree struct {
	S                   string
	ST                  string
	left, right, parent *Tree
	v                   int // total volume
	u                   int // active volume
}

func (x *Tree) GetU() int {
	return x.u
}

func New() *Tree {
	var y Tree
	return &y
}

func (x *Tree) Add(s string) *Tree {
	if x == nil {
		y := New()
		y.S = s
		return y
	}
	if x.S == s {
		return x
	}
	if s < x.S {
		if x.left != nil {
			return x.left.Add(s)
		} else {
			y := New()
			y.parent = x
			y.S = s
			y.ST = "active"
			x.left = y
			y.Plus()
			return x
		}
	}
	if s > x.S {
		if x.right != nil {
			return x.right.Add(s)
		} else {
			y := New()
			y.parent = x
			y.S = s
			y.ST = "active"
			x.right = y
			y.Plus()
			return x
		}
	}
	log.Println("Add exception")
	os.Exit(0)
	return nil
}

func (x *Tree) Draw(ra *rand.Rand) (string, error) {
	if x.u == 0 {
		return "", errors.New("zero vol")
	}
	i := ra.Intn(x.u)
	vl := 0
	if x.left != nil {
		vl = x.left.u
	}
	if i < vl {
		return x.left.Draw(ra)
	}
	if i == vl && x.ST == "active" {
		return x.S, nil
	}
	return x.right.Draw(ra)
}

func (x *Tree) Plus() {
	x.u += 1
	if x.parent != nil {
		x.parent.Plus()
	}
}

func (x *Tree) Minus() {
	x.u -= 1
	if x.parent != nil {
		x.parent.Minus()
	}
}

func (x *Tree) SetInactive(s string) {
	y := x.Find(s)
	if y != nil {
		s0 := y.ST
		if s0 == "active" {
			y.Minus()
			y.ST = "inactive"
		}
	}
}

func (x *Tree) SetActive(s string) {
	y := x.Find(s)
	if y != nil {
		s0 := y.ST
		if s0 == "inactive" {
			y.Plus()
			y.ST = "active"
		}
	}
}

func (x *Tree) Find(s string) *Tree {
	if x == nil {
		return x
	}
	if x.S == s {
		return x
	}
	if s < x.S {
		if x.left != nil {
			return x.left.Find(s)
		} else {
			return nil
		}
	}
	if s > x.S {
		if x.right != nil {
			return x.right.Find(s)
		} else {
			return nil
		}
	}
	log.Println("Find exception")
	os.Exit(0)
	return nil
}

func (x *Tree) Print1() {
	if x != nil {
		log.Println(x.S, x.u) //, x.left, x.right, x.parent)
	} else {
		return
	}
	x.left.Print1()
	x.right.Print1()
}
