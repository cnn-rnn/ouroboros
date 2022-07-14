package matri

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func RandVec(d int, n int, ra *rand.Rand) []int {
	x := make([]int, n)
	c := 0
	for c < d {
		i := ra.Intn(n)
		if x[i] == 1 {
			continue
		} else {
			x[i] = 1
			c++
		}
	}
	return x
}

func RandMat(d int, n int) [][]int {
	var rs = rand.NewSource(time.Now().UnixNano())
	var ra = rand.New(rs)
	x := make([][]int, n)
	for i := 0; i < n; i++ {
		x[i] = RandVec(d, n, ra)
	}
	for i := 0; i < n; i++ {
		x[i][i] = 0
	}
	return x
}

func Compress(x [][]int) [][]int {
	n := len(x)
	y := make([][]int, n)
	for i := 0; i < n; i++ {
		z := make([]int, 0)
		for j := 0; j < len(x[i]); j++ {
			if x[i][j] != 0 {
				z = append(z, j)
			}
		}
		y[i] = z
	}
	return y
}

func Times(x [][]int, y [][]int) [][]int {
	n := len(x)
	if len(x) <= 0 || len(y) <= 0 {
		return make([][]int, 0)
	}
	l := len(x[0])
	m := len(y[0])
	z := make([][]int, n)
	for i := 0; i < n; i++ {
		u := make([]int, m)
		for j := 0; j < m; j++ {
			v := 0
			for k := 0; k < l; k++ {
				v += x[i][k] * y[k][j]
			}
			u[j] = v
		}
		z[i] = u
	}
	return z
}

func Path1(i int, j int, l int, x [][]int, y [][][]int, ra *rand.Rand) int {
	if y[len(y)-1][i][j] == 0 {
		fmt.Println("top level")
		os.Exit(0)
	}
	if l == 0 {
		if y[0][i][j] == 1 {
			return 1
		} else {
			fmt.Println("l=0 A[i][j]=0", y[0][i][j])
			fmt.Println(y[0])
			os.Exit(0)
		}
	}
	d := len(x[i])
	c := 0
	for c < 100 {
		k := ra.Intn(d)
		m := x[i][k]
		if y[0][i][m] == 0 {
			fmt.Println("here")
			os.Exit(0)
		}

		if y[l-1][m][j] > 0 {
			return 1 + Path1(m, j, l-1, x, y, ra)
		}
		c += 1
	}
	fmt.Println("c=", c)
	os.Exit(0)
	return -1
}

func PathShortest(i int, j int, x [][]int, y [][][]int, ra *rand.Rand) int {
	l := len(y) - 2
	for ; l >= 0 && y[l][i][j] > 0; l-- {
	}
	return Path1(i, j, l+1, x, y, ra)
}

func Next(i int, j int, x [][]int, y [][][]int, ra *rand.Rand) int {
	d := len(x[i])
	l := 0
	for ; l < len(y) && y[l][i][j] == 0; l++ {
	}
	if l == len(y) {
		//fmt.Println("matri Next")
		//os.Exit(0)
		return -1
	}
	if l == 0 {
		if y[0][i][j] == 1 {
			return j
		} else {
			fmt.Println("matri here2")
			os.Exit(0)
		}
	}
	c := 0
	for c < 10000 {
		k := ra.Intn(d)
		m := x[i][k]
		if y[0][i][m] == 0 {
			fmt.Println("matri  here1", y)
			os.Exit(0)
		}
		if y[l-1][m][j] > 0 {
			return m
		}
		c += 1
	}
	fmt.Println("matri : here3")
	os.Exit(0)
	return -1
}

func Itinerary1(i int, j int, l int, x [][]int, y [][][]int, ra *rand.Rand) []int {
	if l == 0 {
		if y[0][i][j] == 1 {
			w := make([]int, 1)
			w[0] = j
			return w
		} else {
			fmt.Println("l=0 A[i][j]=0", y[0][i][j])
			fmt.Println(y[0])
			os.Exit(0)
		}
	}
	d := len(x[i])
	c := 0
	for c < 100 {
		k := ra.Intn(d)
		m := x[i][k]
		if y[0][i][m] == 0 {
			fmt.Println("here")
			os.Exit(0)
		}
		if y[l-1][m][j] > 0 {
			w := make([]int, 1)
			w[0] = m
			return append(w, Itinerary1(m, j, l-1, x, y, ra)...)
		}
		c += 1
	}
	fmt.Println("c=", c)
	os.Exit(0)
	return make([]int, 0)
}

func ItineraryShortest(i int, j int, x [][]int, y [][][]int, ra *rand.Rand) []int {
	l := 0
	for ; l < len(y) && y[l][i][j] == 0; l++ {
	}
	return Itinerary1(i, j, l, x, y, ra)
}
