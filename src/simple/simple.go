package simple

import (
	"fmt"
	"io"
	"os"
)

type Simple struct {
	X   io.Reader
	pos int //  current position in t

	N int

	A []byte
	B []byte

	s1 int
	s2 int

	q bool
}

func Init(A []byte, B []byte) *Simple {
	return &Simple{A: A, B: B, s1: 0, s2: 0, q: false}
}

func (x *Simple) Read(y []byte) (int, error) {
	n, err := x.X.Read(y)
	x.N = n
	d := 0
	x.pos = -1

	s10 := x.s1
	s20 := x.s2
	q0 := x.q

	for x.next_state(y[0:n]) {
		if x.s1 <= len(x.A) || (x.s1 == len(x.A)+1 && x.s2 > 0) {
			d += 1
		}
		/*
			if x.q {
				d += 1
				x.q = false
			}
		*/
	}

	z := make([]byte, d)
	d = 0
	x.pos = -1
	x.s1 = s10
	x.s2 = s20
	x.q = q0

	for x.next_state(y[0:n]) {
		if x.s1 <= len(x.A) || (x.s1 == len(x.A)+1 && x.s2 > 0) {
			z[d] = y[x.pos]
			d += 1
		}
		/*
			if x.q {
				z[d] = ' '
				d += 1
				x.q = false
			}
		*/
	}

	if d > len(y) {
		fmt.Println("suka", d, len(y), string(y), "\nA=", string(x.A), "B=", string(x.B))
		os.Exit(0)
	}

	for i := 0; i < d; i++ {
		y[i] = z[i]
	}

	return d, err
}

func (x *Simple) SetReader(r io.Reader) {
	x.X = r
}

func (x *Simple) next_state(y []byte) bool {

	if x.pos >= x.N-1 {
		return false
	}

	x.pos += 1

	if x.s1 == 0 && y[x.pos] == x.A[0] {
		x.s1 = 1
		return true
	}

	for i := 0; i < len(x.A); i++ {
		if x.s1 == i {
			if y[x.pos] == x.A[i] {
				x.s1 += 1
			} else {
				x.s1 = 0
			}
			return true
		}
	}

	if x.s1 == len(x.A) {
		x.s1 += 1
	}

	if x.s1 == len(x.A)+1 {
		if x.s2 == 0 && y[x.pos] == x.B[0] {
			x.s2 = 1
			return true
		}
		for j := 1; j < len(x.B); j++ {
			if x.s2 == j {
				if y[x.pos] == x.B[j] {
					x.s2 += 1
				} else {
					x.s2 = 0
				}
				if j < len(x.B)-1 {
					return true
				}
			}
		}
		if x.s2 == len(x.B) {
			x.s1 = 0
			x.s2 = 0
			//			x.q = true
		}
	}
	return true
}
