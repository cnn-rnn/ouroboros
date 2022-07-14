package links

import (
	"fmt"
	"io"
)

type Links struct {
	X   io.Reader
	pos int //  current position in t

	name string

	N int

	w io.Writer

	s1 int
	s2 int

	u []byte
}

func Init() *Links {
	fmt.Println("Init")
	return &Links{}
}

func (x *Links) grow() {
	n := cap(x.u)
	t := make([]byte, n+1)
	for i := 0; i < n; i++ {
		t[i] = x.u[i]
	}
	x.u = t
}

func (x *Links) Read(y []byte) (int, error) {
	n, err := x.X.Read(y)
	x.N = n
	B := false
	d := 0
	x.pos = -1

	for x.next_state(y[0:n]) {

		//fmt.Println(x.pos, string(y[x.pos]), x.s1, x.s2)

		if x.s2 == 6 {
			B = true
			if d+1 >= len(x.u) {
				x.grow()
			}
			x.u[d] = y[x.pos]
			d += 1
		}
		if !(x.s2 == 6) && B {
			if d+1 >= len(x.u) {
				x.grow()
			}
			x.u[d] = '\n'
			x.w.Write(x.u[0 : d+1])
			B = false
			d = 0
		}
	}
	return n, err
}

func (x *Links) SetReader(r io.Reader) {
	x.X = r
}
func (x *Links) SetWriter(w io.Writer) {
	x.w = w
}
func (x *Links) Reset() {
	x.reset()
}

func (x *Links) next_state(y []byte) bool {

	if x.pos >= x.N-1 {
		return false
	}

	x.pos += 1

	if y[x.pos] == '<' {
		x.s1 = 1
		return true
	}
	if x.s1 == 1 {
		if y[x.pos] == 'a' {
			x.s1 = 2
		} else {
			x.s1 = 0
		}
		return true
	}
	if x.s1 == 2 {
		if y[x.pos] == '>' {
			x.reset()
			return true
		}

		if x.s2 == 6 {
			if y[x.pos] == '"' || y[x.pos] == '\'' {
				x.reset()
				return true
			}
		}
		if x.s2 == 5 {
			x.s2 = 6
			return true
		}
		if x.s2 == 4 {
			if y[x.pos] == '"' || y[x.pos] == '\'' {
				x.s2 = 5
			}
			return true
		}
		if x.s2 == 3 {
			if y[x.pos] == 'f' {
				x.s2 = 4
			} else {
				x.s2 = 0
			}
			return true
		}
		if x.s2 == 2 {
			if y[x.pos] == 'e' {
				x.s2 = 3
			} else {
				x.s2 = 0
			}
			return true
		}
		if x.s2 == 1 {
			if y[x.pos] == 'r' {
				x.s2 = 2
			} else {
				x.s2 = 0
			}
			return true
		}
		if x.s2 == 0 {
			if y[x.pos] == 'h' {
				x.s2 = 1
			}
			return true
		}
	}
	return true
}

func (x *Links) reset() {
	x.s1 = 0
	x.s2 = 0
}
