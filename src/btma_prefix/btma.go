package btma_prefix

// store info  ( such as the number of uncrawled pages , and do Rand accordinly

import (
	//	"crypto/sha256"
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	//"sync"
	//"time"
)

var N0 = 4096
var L = 4
var LV = 2
var LN = 2

var P = 5

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

/*
func hash(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash[:])
}
*/

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

func (x *BTM) SetRa(ra *rand.Rand) {
	(*x.ra) = *ra
}

func (x *BTM) GetL() int {
	return len(*x.x)
}

/*
func (x *BTM) GetU() int64 {
	return x.GetNumber(int64(4 * L))
}
*/
func (x *BTM) GetU() int64 {
	return x.GetNumber1(int64(4*L), LV)
}
func (x *BTM) GetV() int64 {
	return x.GetNumber1(int64(4*L+LV), LV)
}

func (x *BTM) GetX() *[]uint8 {
	return x.x
}

func Create(ra *rand.Rand, dir string, s string) *BTM {
	f, e := os.OpenFile(dir+"/"+s, os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		//log.Println("btma.Create e=", e)
		return nil
	}
	f.Write(make([]byte, N0))
	fd := int(f.Fd())
	x, e1 := syscall.Mmap(fd, 0, int(N0), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e1 != nil {
		//log.Println("btma.Create e1=", e1)
		return nil
	}
	return &BTM{host: s, f: f, fd: int(f.Fd()), x: &x, E: 0, N: int64(N0), ra: ra}
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

func Open(ra *rand.Rand, dir string, s string) *BTM {
	f, e := os.OpenFile(dir+"/"+s, os.O_APPEND|os.O_RDWR, 0644)
	if e != nil {
		//log.Println("btma.Open e=", e, dir, s)
		return nil
	}
	fd := int(f.Fd())
	fi, _ := f.Stat()
	N1 := fi.Size()
	x, e1 := syscall.Mmap(fd, 0, int(N1), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e1 != nil {
		//log.Println("btma.Open e1=", e1)
		return nil
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
		log.Println("err", err)
		return nil
	}
	return Open(ra, dir, s)
}

func (x *BTM) Close() {
	x.f.Sync()
	syscall.Munmap(*x.x)
	x.f.Close()
}

func (x *BTM) Expand(n int) {
	syscall.Munmap(*x.x)
	k := n/N0 + 1
	N1 := k * N0
	x.N += int64(N1)
	x.f.Seek(x.N, 0)
	x.f.Write(make([]byte, N1))
	var e error
	*x.x, e = syscall.Mmap(x.fd, 0, int(x.N), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if e != nil {
		log.Println("btma_pre.Expand e=", e)
	}
}

func (x *BTM) NewRecord(parent int64, ss string) {
	s := make([]uint8, P+1+len(ss)+1)
	for i := 0; i < P; i++ {
		s[i] = 0x11
	}
	s[P] = '+'
	s[P+1+len(ss)] = '\n'
	for i := 0; i < len(ss); i++ {
		s[P+1+i] = ss[i]
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
	a := i64_u8(0, LV)
	b := i64_u8(0, LV)
	u = append(u, a...)
	u = append(u, b...)
	c := i64_u8(int64(len(ss)), LN)
	u = append(u, c...)
	u = append(u, s...)
	if x.E+int64(len(u)) > x.N {
		x.Expand(len(u))
	}
	n0 := x.E
	x.Write(x.E, u)
	x.E += int64(len(u))
	x.AddV(n0)
	x.AddU(n0)
}

func (x *BTM) Write(m int64, u []uint8) {
BEG:
	if int64(len(*x.x)) <= m+int64(len(u))+1 || int64(x.N) <= m+int64(len(u))+1 {
		x.Expand(len(u))
	}
	if int64(len(*x.x)) <= m+int64(len(u))+1 || int64(x.N) <= m+int64(len(u))+1 {
		log.Println("did not expand", len(*x.x), x.E, m+int64(len(u))+1)
		goto BEG
	}

	for i := 0; i < len(u); i++ {
		(*x.x)[m+int64(i)] = u[i]
	}
}

func (x *BTM) Add(n int64, s string) int64 {
	if len(s) > 10000 {
		log.Println("string too long", len(s))
		return -5
	}
	if x == nil {
		log.Println("btma_pre x=nil")
		os.Exit(0)
	}
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
			x.Write(n+int64(L), i64_u8(x.E, L))
			e := x.E
			x.NewRecord(n, s)
			return e
		} else {
			return x.Add(u8_i64(l), s)
		}
	}
	if s > s1 {
		r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
		if r[L-1] == '-' {
			x.Write(n+2*int64(L), i64_u8(x.E, L))
			e := x.E
			x.NewRecord(n, s)
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
	if int(m+int64(LN+P+1)+int64(k)) >= len(*x.x) {
		x.Remap()
		if int(m+int64(LN+P+1)+int64(k)) >= len(*x.x) {
			log.Println("GetContent: out of bounds k =", k)
			return make([]byte, 0)
		}
	}
	s := (*x.x)[m+int64(LN+P+1) : m+int64(LN+P+1)+int64(k)]
	return s
}

func (x *BTM) GetRecord(n int64) Record {
	m := n + 4*int64(L)
	u := u8_i64((*x.x)[m : m+int64(LV)])
	v := u8_i64((*x.x)[m+int64(LV) : m+2*int64(LV)])
	d := string((*x.x)[m+int64(LV+LV)+int64(LN)])
	k := x.GetNumber1(m+int64(LV+LV), LN)
	return Record{int(u), int(v), d, string((*x.x)[m+int64(LV+LV+LN+P+1) : m+int64(LV+LV+LN+P+1)+k])}
}

func (x *BTM) GetNumber1(n int64, l int) int64 {
BEG:
	if n+int64(l) >= int64(len(*x.x)) {
		x.Remap()
	}

	y := int64(0)
	w := 0
	for i := 0; i < l; i++ {
		if int(n)+i >= len(*x.x) {
			//log.Println("btma.GetNumber1 goto BEG")
			goto BEG
		}
		y += int64((*x.x)[n+int64(i)]) << w
		w += 8
	}
	return y
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
		log.Println("vl+vt+vr != v", vl, vt, vr, v, string(x.GetContent(n)), (*x.x)[n:n+4*int64(L)])
		// /os.Exit(0)
	}
	if v == 0 {
		return ""
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
		log.Println("ul+ut+ur != u", ul, ut, ur, u, string(x.GetContent(n)))
		//os.Exit(0)
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

func (x *BTM) RandVNoChangeV(n int64) (string, error) {
	if x == nil || x.E == 0 {
		return "", nil
	}

	if int(n+int64(4*L+LV+LV+LN+P)) > len(*x.x) {
		x.Remap()
		if int(n+int64(4*L+LV+LV+LN+P)) > len(*x.x) {
			return "", errors.New("length out of range")
		}
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
	if (*x.x)[n+int64(4*L+LV+LV+LN+P)] == '+' {
		vt = 1
	}
	v := int(x.GetNumber1(n+4*int64(L)+int64(LV), LV))
	if vl+vt+vr != v { //  vl+vr+ this == v_this
		log.Println("randnochangev : vl+vt+vr != v; n=" + strconv.Itoa(int(n)) + " v=" + strconv.Itoa(v) +
			" vr=" + strconv.Itoa(vr) + " vl=" + strconv.Itoa(vl) + " vt=" + strconv.Itoa(vt))
		//		return "", errors.New("vl+vt+vr != v")
	}
	if v <= 0 {
		return "", nil
	}
	//log.Println(v)
	u := x.ra.Intn(v)
	if u < vl {
		//	log.Println("left", u8_i64(l))
		return x.RandVNoChangeV(u8_i64(l))
	}
	if u == vl && vt == 1 {
		//	log.Println("return", string(x.GetContent(n)))
		return string(x.GetContent(n)), nil
	}
	//log.Println("right", u8_i64(r))
	return x.RandVNoChangeV(u8_i64(r))
}

func (x *BTM) SetActive(s string) {
	n := x.Find(0, s)
	if n < 0 {
		x.Add(0, s)
		return
	}
	(*x.x)[n+int64(4*L+LV+LV+LN+P)] = '+'
	x.UpdateV(n)
}

func (x *BTM) SetInactive(s string) {
	n := x.Find(0, s)
	if n < 0 {
		//		log.Println("not found")
		return
	} else {
	}
	(*x.x)[n+int64(4*L+LV+LV+LN+P)] = '-'
	x.UpdateV(n)
}

func (x *BTM) SetState(y byte, s string) {
	n := x.Find(0, s)
	(*x.x)[n+4*int64(L)+2*int64(LV)+int64(LN)] = y
}

func (x *BTM) UpdateV(n int64) {
	if x == nil || x.E == 0 {
		return
	}
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	vl := int64(0)
	if l[L-1] != '-' {
		vl = x.GetNumber1(u8_i64(l)+int64(4*L+LV), LV)
	}
	vr := int64(0)
	if r[L-1] != '-' {
		vr = x.GetNumber1(u8_i64(r)+int64(4*L+LV), LV)
	}
	vt := int64(0)
	if (*x.x)[n+int64(4*L+LV+LV+LN+P)] == '+' {
		vt = 1
	}
	v := vl + vt + vr
	x.Write(n+int64(4*L+LV), i64_u8(v, LV))
	p := (*x.x)[n : n+int64(L)]

	//	log.Println("p=", p)

	if p[L-1] != '-' {
		x.UpdateV(u8_i64(p))
	}
}

func (x *BTM) UpdateU(n int64) {
	if x == nil || x.E == 0 {
		return
	}
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	vl := int64(0)
	if l[L-1] != '-' {
		vl = x.GetNumber1(u8_i64(l)+int64(4*L+LV), LV)
	}
	vr := int64(0)
	if r[L-1] != '-' {
		vr = x.GetNumber1(u8_i64(r)+int64(4*L+LV), LV)
	}
	vt := int64(1)
	v := vl + vt + vr
	x.Write(n+int64(4*L), i64_u8(v, LV))
	p := (*x.x)[n : n+int64(L)]

	if p[L-1] != '-' {
		x.UpdateU(u8_i64(p))
	}
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
		log.Println("check vl+vt+vr != v", vl, vt, vr, v, string(x.GetContent(n)), (*x.x)[n:n+4*int64(L)])
		//os.Exit(0)
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
	if x == nil || x.E == 0 {
		return make([]string, 0)
	}

	s := string(x.GetContent(n))

	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]

	//log.Println(string(s),l,r)

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

func (x *BTM) AsListActive(n int64) []string {
	s := string(x.GetContent(n))
	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	w := make([]string, 0)
	if l[L-1] != '-' {
		u := x.AsList(u8_i64(l))
		w = append(w, u...)
	}
	if (*x.x)[n+int64(4*L+LV+LV+LN+P)] == '+' {
		w = append(w, s)
	}
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
	if x == nil || x.E == 0 {
		return -1
	}
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

func (x *BTM) Print(n int64) {

	l := (*x.x)[n+int64(L) : n+2*int64(L)]
	r := (*x.x)[n+2*int64(L) : n+3*int64(L)]
	vl := -1
	if l[L-1] != '-' {
		vl = int(x.GetNumber1(u8_i64(l)+4*int64(L)+int64(LV), LV))
	}
	vr := -1
	if r[L-1] != '-' {
		vr = int(x.GetNumber1(u8_i64(r)+4*int64(L)+int64(LV), LV))
	}

	//(*x.x)[n+int64(4*L+LV+LV+LN+P)]
	if l[L-1] != '-' {
		x.Print(u8_i64(l))
	}

	log.Println("vl", vl, "vr", vr, "vt", string((*x.x)[n+int64(4*L+LV+LV+LN+P)]), string(x.GetContent(n)))

	if r[L-1] != '-' {
		x.Print(u8_i64(r))
	}

}
