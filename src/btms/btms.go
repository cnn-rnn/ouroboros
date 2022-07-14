package btms

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"syscall"
	//"sync"
	//"time"
)

//this is PLRTUV(len s)s         No status or end \n

var N0 = 10000
var L = 5
var LV = 5
var LN = 5

type BTM struct {
	host string
	f    *os.File
	fd   int
	x    *[]uint8
	E, N int64
	ra   *rand.Rand
}

type Record struct {
	u int
	v int
	d string
	s string
}

func (x *Record) GetS() string {
	return x.s
}

func s_u8(s string) []uint8 {
	n := len(s)
	x := make([]uint8, n)
	for i := 0; i < n; i++ {
		x[i] = uint8(s[i])
	}
	return x
}

func u8_s(x []uint8) string {
	s := ""
	for i := 0; i < len(x); i++ {
		s += string(byte(x[i]))
	}
	return s
}

func i64_u8(n int64, l int) []uint8 {
	x := make([]uint8, l)
	i := 0
	for n > 0 {
		y := n % 256
		z := uint8(y)
		x[i] = z
		n /= 256
		i += 1
	}
	return x
}

func u8_i64(x []uint8) int64 {
	n := len(x)
	w := int64(1)
	y := int64(0)
	for i := 0; i < n; i++ {
		y += w * int64(x[i])
		w *= 256
	}
	return y
}

func (x *BTM) add1(n int64) {
	b := (*x.x)[n] == 255
	(*x.x)[n] += 1
	if b {
		x.add1(n + 1)
	}
}

func (x *BTM) sub1(n int64) {
	b := (*x.x)[n] == 0
	if (*x.x)[n] == 0 {
		(*x.x)[n] = 255
	} else {
		(*x.x)[n] -= 1
	}
	if b {
		x.sub1(n + 1)
	}
}

func (x *BTM) GetL() int {
	return len(*x.x)
}

func (x *BTM) GetX() *[]uint8 {
	return x.x
}

func (x *BTM) GetU(n int64) int64 {
	return x.GetNumber1(n+int64(4*L), LV)
}

func Create(ra *rand.Rand, dir string, s string) *BTM {
	f, e := os.OpenFile(dir+"/"+s, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		fmt.Println("btms.Create e=", e)
		os.Exit(0)
	}
	f.Write(make([]byte, N0))
	fd := int(f.Fd())
	x, e1 := syscall.Mmap(fd, 0, int(N0), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e1 != nil {
		fmt.Println("btms.Create e1=", e1)
		os.Exit(0)
	}
	return &BTM{s, f, int(f.Fd()), &x, 0, int64(N0), ra}
}

func Open(ra *rand.Rand, dir string, s string) *BTM {
	f, e := os.OpenFile(dir+"/"+s, os.O_APPEND|os.O_RDWR, 0644)
	if e != nil {
		fmt.Println("btms.Open e=", e, dir, s)
		return nil
	}
	fd := int(f.Fd())
	fi, _ := f.Stat()
	N1 := fi.Size()
	x, e1 := syscall.Mmap(fd, 0, int(N1), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e1 != nil {
		fmt.Println("btms.Open e1=", e1)
		return Create(ra, dir, s)
		os.Exit(0)
	}
	E := int64(N1)
	for i := N1 - 1; i >= 0; i-- {
		if x[i] != 0 {
			break
		}
		E--
	}
	return &BTM{s, f, int(f.Fd()), &x, E, int64(fi.Size()), ra}
}

func OpenOrCreate(ra *rand.Rand, dir string, s string) *BTM {
	if _, err := os.Stat(dir + "/" + s); os.IsNotExist(err) {
		return Create(ra, dir, s)
	}
	return Open(ra, dir, s)
}

func OpenIfExists(ra *rand.Rand, dir string, s string) *BTM {
	if _, err := os.Stat(dir + "/" + s); os.IsNotExist(err) {
		return nil
	}
	return Open(ra, dir, s)
}

func (x *BTM) Close() {
	x.f.Sync()
	syscall.Munmap(*x.x)
	x.f.Close()
}

func (x *BTM) Expand() {
	syscall.Munmap(*x.x)
	x.N += int64(N0)
	x.f.Seek(x.N, 0)
	x.f.Write(make([]byte, N0))
	var e error
	*x.x, e = syscall.Mmap(x.fd, 0, int(x.N), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e != nil {
		fmt.Println("btm.Expand e=", e)
		os.Exit(0)
	}
}

func (x *BTM) Remap() {
	fd := int(x.f.Fd())
	fi, _ := x.f.Stat()
	N1 := fi.Size()

	syscall.Munmap(*x.x)
	y, e1 := syscall.Mmap(fd, 0, int(N1), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e1 != nil {
		log.Fatal("btm remap", e1)
	}

	i := N1 - 1
	for ; i > 0 && y[i] == '\x00'; i-- {
	}
	x.E = i

	x.x = &y
}

func (x *BTM) NewRecord(parent int64, ss string) {
	s := make([]uint8, 2+len(ss)+1)
	s[0] = '-'
	s[1] = 0
	s[len(ss)+2] = '\n'
	for i := 0; i < len(ss); i++ {
		s[i+2] = ss[i]
	}
	var p []uint8
	if parent < 0 {
		p = i64_u8(1, L)
		p[L-1] = 45
	} else {
		p = i64_u8(parent, L)
	}
	l := i64_u8(0, L)
	l[L-1] = '-'
	r := i64_u8(0, L)
	r[L-1] = '-'
	t := i64_u8(x.E, L)
	u := p
	u = append(u, l...)
	u = append(u, r...)
	u = append(u, t...)
	a := i64_u8(0, LV) // why 0?   shouldnt it be 1?  addU(this)
	b := i64_u8(0, LV)
	u = append(u, a...)
	u = append(u, b...)
	c := i64_u8(int64(len(s)), LN)
	u = append(u, c...)
	u = append(u, s...)
	for x.E+int64(len(u)) > x.N {
		x.Expand()
	}
	n0 := x.E
	x.Write(x.E, u)
	x.E += int64(len(u))

	if int64(len(*x.x)) > x.N {
		fmt.Println("new node ", len(*x.x), x.N)
		os.Exit(0)
	}

	x.AddV(n0)
	x.AddU(n0)
}

func (x *BTM) Write(m int64, u []uint8) {
	for i := 0; i < len(u); i++ {
		(*x.x)[m+int64(i)] = u[i]
	}
}

func (x *BTM) Add(n int64, s string) int64 {
	if x.E == 0 {
		x.NewRecord(-1, s)
		return 0
	}
	s1 := string(x.GetContent(n))
	if s == s1 {
		return -1
	}
	if s < s1 {
		l := (*x.x)[n+int64(L) : n+2*int64(L)]
		if l[L-1] == '-' {
			e := x.E
			x.NewRecord(n, s)
			x.Write(n+int64(L), i64_u8(e, L))
			return e
		} else {
			return x.Add(u8_i64(l), s)
		}
	}
	if s > s1 {
		r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
		if r[L-1] == '-' {
			e := x.E
			x.NewRecord(n, s)
			x.Write(n+2*int64(L), i64_u8(e, L))
			return e
		} else {
			return x.Add(u8_i64(r), s)
		}
	}
	return -2
}

func (x *BTM) GetContent(n int64) []uint8 {
	m := n + 4*int64(L) + 2*int64(LV)
	k := x.GetNumber1(m, LN)

	if k == 0 {
		return make([]byte, 0)
	}

	if m+int64(LN)+int64(k)-1 >= int64(len(*x.x)) || m+int64(LN)+int64(k)-1 >= x.N {
		x.Remap()
	}
	if m+int64(LN)+int64(k)-1 >= int64(len(*x.x)) || m+int64(LN)+int64(k)-1 >= x.N {
		log.Println(m+int64(LN)+int64(k)-1, int64(len(*x.x)), m+int64(LN)+int64(k)-1 >= int64(len(*x.x)))
		return make([]byte, 0)
	}

	s := (*x.x)[m+int64(LN)+2 : m+int64(LN)+int64(k)-1]
	return s
}

func (x *BTM) GetRecord(n int64) Record {
	m := n + 4*int64(L)
	u := u8_i64((*x.x)[m : m+int64(LV)])
	v := u8_i64((*x.x)[m+int64(LV) : m+2*int64(LV)])
	d := string((*x.x)[m+2*int64(LV)+int64(LN)])
	k := x.GetNumber1(m+2*int64(LV), LN)
	return Record{int(u), int(v), d, string((*x.x)[m+2*int64(LV)+int64(LN)+2 : m+2*int64(LV)+int64(LN)+2+k-3])}
}

func (x *BTM) GetNumber(n int64) int64 {
	y := int64(0)
	w := int64(1)
	for (*x.x)[n] != 0 {
		y += int64((*x.x)[n]) * w
		w *= 256
		n += 1
	}
	return y
}

/*
func (x * BTM)GetNumber1(n int64,l int)int64{
y := int64(0)
w := int64(1)
for i :=0;i<l;i++{
   y += int64((*x.x)[n+int64(i)])*w
   w *= 256
}
return y
}
*/

func (x *BTM) GetNumber1(n int64, l int) int64 {
	y := int64(0)
	w := int64(1)
	z := (*x.x)[n : n+int64(l)]
	for i := 0; i < l; i++ {
		y += int64(z[i]) * w
		w *= 256
	}
	return y
}

func (x *BTM) GetV(n int64) int64 {
	return x.GetNumber1(n+4*int64(L)+int64(LV), LV)
}

func (x *BTM) Rand(n int64) string {
	if x == nil || x.E == 0 {
		return ""
	}
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	vl := 0
	if l[L-1] != '-' {
		vl = int(x.GetNumber1(u8_i64(l)+4*int64(L)+int64(LV), LV))
	}
	vr := 0
	if r[L-1] != '-' {
		vr = int(x.GetNumber1(u8_i64(r)+4*int64(L)+int64(LV), LV))
	}
	vt := 0
	if (*x.x)[n+4*int64(L)+2*int64(LV)+int64(LN)] == '-' {
		vt = 1
	}
	v := int(x.GetNumber1(n+4*int64(L)+int64(LV), LV))
	if vl+vt+vr != v { //  vl+vr+ this == v_this
		fmt.Println("vl+vt+vr != v", vl, vt, vr, v, string(x.GetContent(n)), (*x.x)[n:n+4*int64(L)])
		os.Exit(0)
	}
	if v == 0 {
		return ""
	}
	if v <= 0 {
		fmt.Println("WFT", v, x.host)
		os.Exit(0)
	}
	u := x.ra.Intn(v)
	if u < vl {
		return x.Rand(u8_i64(l))
	}
	if u == vl && vt == 1 {
		(*x.x)[n+4*int64(L)+2*int64(LV)+int64(LN)] = '+'
		x.SubtractV(n)
		return string(x.GetContent(n))
	}
	return x.Rand(u8_i64(r))
}

func (x *BTM) RandU(n int64) string {
	if x == nil || x.E == 0 {
		return ""
	}
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	ul := 0
	if l[L-1] != '-' {
		ul = int(x.GetNumber1(u8_i64(l)+4*int64(L), LV))
	}
	ur := 0
	if r[L-1] != '-' {
		ur = int(x.GetNumber1(u8_i64(r)+4*int64(L), LV))
	}
	ut := 1
	u := int(x.GetNumber1(n+4*int64(L), LV))
	if ul+ut+ur != u { //  vl+vr+ this == v_this
		fmt.Println("ul+ut+ur != u", ul, ut, ur, u, string(x.GetContent(n)))
		os.Exit(0)
	}
	if u == 0 {
		return ""
	}
	w := x.ra.Intn(u)
	if w < ul {
		return x.RandU(u8_i64(l))
	}
	if w == ul {
		return string(x.GetContent(n))
	}
	return x.RandU(u8_i64(r))
}

func (x *BTM) Check(n int64) {
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	vl := 0
	if l[L-1] != '-' {
		vl = int(x.GetNumber1(u8_i64(l)+4*int64(L)+int64(LV), LV))
	}
	vr := 0
	if r[L-1] != '-' {
		vr = int(x.GetNumber1(u8_i64(r)+4*int64(L)+int64(LV), LV))
	}
	vt := 0
	if (*x.x)[n+4*int64(L)+2*int64(LV)+int64(LN)] == '-' {
		vt = 1
	}
	v := int(x.GetNumber1(n+4*int64(L)+int64(LV), LV))
	if vl+vt+vr != v { //  vl+vr+ this == v_this
		fmt.Println("check vl+vt+vr != v", vl, vt, vr, v, string(x.GetContent(n)), (*x.x)[n:n+4*int64(L)])
		os.Exit(0)
	}
	if l[L-1] != '-' {
		x.Check(u8_i64(l))
	}
	if r[L-1] != '-' {
		x.Check(u8_i64(r))
	}
}

func (x *BTM) SubtractV(n int64) {
	x.sub1(n + 4*int64(L) + int64(LV))
	p := (*x.x)[n : n+int64(L)]
	if p[L-1] != '-' {
		x.SubtractV(u8_i64(p))
	}
}

func (x *BTM) AddV(n int64) {
	x.add1(n + 4*int64(L) + int64(LV))
	p := (*x.x)[n : n+int64(L)]
	if p[L-1] != '-' {
		x.AddV(u8_i64(p))
	}
}

func (x *BTM) AddU(n int64) {
	x.add1(n + 4*int64(L))
	if (*x.x)[n+int64(L)-1] != '-' {
		x.AddU(u8_i64((*x.x)[n : n+int64(L)]))
	}
}

func (x *BTM) AsList(n int64) []string {
	s := string(x.GetContent(n))
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	w := make([]string, 0)
	if l[L-1] != '-' {
		u := x.AsList(u8_i64(l))
		w = append(w, u...)
	}
	w = append(w, s)
	if r[L-1] != '-' {
		u := x.AsList(u8_i64(r))
		w = append(w, u...)
	}
	return w
}

func (x *BTM) AsListR(n int64) []Record {
	if x.E == 1 {
		return make([]Record, 0)
	}
	s := x.GetRecord(n)
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	w := make([]Record, 0)
	if l[L-1] != '-' {
		u := x.AsListR(u8_i64(l))
		w = append(w, u...)
	}
	w = append(w, s)
	if r[L-1] != '-' {
		u := x.AsListR(u8_i64(r))
		w = append(w, u...)
	}
	return w
}

func (x *BTM) Find(n int64, s string) int64 {
	s1 := string(x.GetContent(n))
	if s1 == s {
		return n
	}
	if s < s1 {
		l := (*x.x)[n+int64(L) : n+2*int64(L)]
		if l[L-1] != '-' {
			return x.Find(u8_i64(l), s)
		} else {
			return -1
		}
	}
	if s > s1 {
		r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
		if r[L-1] != '-' {
			return x.Find(u8_i64(r), s)
		} else {
			return -1
		}
	}
	return -2
}

func (x *BTM) GetMin(n int64) string {
	if x.E == 0 {
		return ""
	}
	if (*x.x)[n+2*int64(L)-1] == '-' {
		return string(x.GetContent(n))
	}
	ll := u8_i64((*x.x)[n+int64(L) : n+2*int64(L)])
	return x.GetMin(ll)
}

func (x *BTM) GetMax(n int64) string {
	if x.E == 0 {
		return ""
	}
	if (*x.x)[n+3*int64(L)-1] == '-' {
		return string(x.GetContent(n))
	}
	return x.GetMax(u8_i64((*x.x)[n+2*int64(L) : n+3*int64(L)]))
}

func (x *BTM) GetNext(n int64, s string) string {
	if x.E == 0 {
		return ""
	}
	m := x.Find(n, s)
	if m == -1 {
		return ""
	}
	if (*x.x)[m+3*int64(L)-1] != '-' {
		return x.GetMin(u8_i64((*x.x)[m+2*int64(L) : m+3*int64(L)]))
	}
	p := (*x.x)[m : m+int64(L)]
	s1 := string(x.GetContent(u8_i64(p)))
	for s1 < s && p[L-1] != '-' {
		m = u8_i64(p)
		p = (*x.x)[m : m+int64(L)]
		if p[L-1] == '-' {
			return x.GetMin(0)
		}
		s1 = string(x.GetContent(u8_i64(p)))
	}
	return s1
}
