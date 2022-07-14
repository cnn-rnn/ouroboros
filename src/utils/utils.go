package utils

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

func Check(e error, s string) {
	if e != nil {
		log.Fatal(e, s)
	}
}

func Rand_bytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func Rand_bytes1(ra *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(ra.Intn(256))
	}
	return b
}

func Generate_id(ra *rand.Rand) string {
	b := Rand_bytes1(ra, 32)
	y := ""
	for i := 0; i < 32; i++ {
		x := int(b[i]) % 16
		if x == 0 {
			y += "0"
		}
		if x == 1 {
			y += "1"
		}
		if x == 2 {
			y += "2"
		}
		if x == 3 {
			y += "3"
		}
		if x == 4 {
			y += "4"
		}
		if x == 5 {
			y += "5"
		}
		if x == 6 {
			y += "6"
		}
		if x == 7 {
			y += "7"
		}
		if x == 8 {
			y += "8"
		}
		if x == 9 {
			y += "9"
		}
		if x == 10 {
			y += "a"
		}
		if x == 11 {
			y += "b"
		}
		if x == 12 {
			y += "c"
		}
		if x == 13 {
			y += "d"
		}
		if x == 14 {
			y += "e"
		}
		if x == 15 {
			y += "f"
		}
	}
	return y
}

func Generate_id_long(ra *rand.Rand, n int) string {
	b := Rand_bytes1(ra, n)
	y := ""
	for i := 0; i < n; i++ {
		x := int(b[i]) % 16
		if x == 0 {
			y += "0"
		}
		if x == 1 {
			y += "1"
		}
		if x == 2 {
			y += "2"
		}
		if x == 3 {
			y += "3"
		}
		if x == 4 {
			y += "4"
		}
		if x == 5 {
			y += "5"
		}
		if x == 6 {
			y += "6"
		}
		if x == 7 {
			y += "7"
		}
		if x == 8 {
			y += "8"
		}
		if x == 9 {
			y += "9"
		}
		if x == 10 {
			y += "a"
		}
		if x == 11 {
			y += "b"
		}
		if x == 12 {
			y += "c"
		}
		if x == 13 {
			y += "d"
		}
		if x == 14 {
			y += "e"
		}
		if x == 15 {
			y += "f"
		}
	}
	return y
}

func MakeDir0(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		em := os.Mkdir(path, 0700)
		if em != nil {
			log.Fatal("dir not made ", em)
		}
	}
}

func B_i(x []byte) int {
	return int(x[0]) + 256*int(x[1])
}

func I_b(n int) []byte {
	x := make([]byte, 2)
	x[0] = byte(n % 256)
	x[1] = byte(n / 256)
	return x
}

func Url_Id(s string) string {
	hash := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", hash[:])
}

func Connect(ip string, dt time.Duration) (net.Conn, error) {
	t0 := time.Now()
BEG:
	dialer := net.Dialer{Timeout: dt}
	conn, e := dialer.Dial("tcp", ip)
	if e != nil {
		//log.Println("cannot connect to ip", ip, "e =", e)
		time.Sleep(time.Second)
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			time.Sleep(time.Duration(1e+8))
			goto BEG
		} else {
			return nil, e
		}
	}
	return conn, nil
}

func GuaranteedWrite(conn net.Conn, r *bufio.Reader, x []byte, dt time.Duration) error {

	if conn == nil {
		return errors.New("your connection is nil")
	}
	if r == nil {
		r = bufio.NewReader(conn)
	}

	n := len(x)
	n2 := n % 256
	n /= 256
	n1 := n % 256
	n /= 256
	n0 := n % 256
	n /= 256
	if n > 0 {
		return errors.New("string too long")
	}
	n2c := byte(n2)
	n1c := byte(n1)
	n0c := byte(n0)
	var y []byte
	y = append(y, n0c)
	y = append(y, n1c)
	y = append(y, n2c)
	y = append(y, x...)
	y = append(y, 0x11)
	t0 := time.Now()
BEG:
	conn.Write(y)
	s, e := r.ReadBytes(0x11)
	if e != nil {
		log.Println("utils: readbytes error", e)
	}
	if len(s) <= 1 || string(s[0:len(s)-1]) != "success" {
		log.Println("utils write uncussesful", s)
	}
	if e != nil || len(s) <= 1 || string(s[0:len(s)-1]) != "success" {
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return errors.New("utils:error reading from connection")
		}
	}
	return nil
}

func GuaranteedWriteFile(conn net.Conn, r *bufio.Reader, fname string, dt time.Duration) error {

	if conn == nil {
		return errors.New("your connection is nil")
	}
	if r == nil {
		r = bufio.NewReader(conn)
	}

	f, e := os.OpenFile(fname, os.O_RDONLY, 0644)
	if e != nil {
		return e
	}
	defer f.Close()

	fi, e := f.Stat()
	n := fi.Size()

	n2 := n % 256
	n /= 256
	n1 := n % 256
	n /= 256
	n0 := n % 256
	n /= 256
	if n > 0 {
		return errors.New("file too long")
	}
	n2c := byte(n2)
	n1c := byte(n1)
	n0c := byte(n0)
	var y []byte
	y = append(y, n0c)
	y = append(y, n1c)
	y = append(y, n2c)
	//	y = append(y, x...)
	//	y = append(y, 0x11)

	t0 := time.Now()
	cuf := make([]byte, 4096)
	f.Seek(0, 0)
	buf := bufio.NewReader(f)

BEG:
	conn.Write(y)
	for {
		m, e := buf.Read(cuf)
		conn.Write(cuf[0:m])
		if e != nil {
			break
		}
	}
	conn.Write(append(make([]byte, 0), 0x11))
	s, e := r.ReadBytes(0x11)
	if e != nil {
		//log.Println("utils: readbytes error", e)
	}
	if e != nil || len(s) <= 1 || string(s[0:len(s)-1]) != "success" {
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return errors.New("utils:error reading from connection")
		}
	}
	return nil
}

func GuaranteedWriteFileToAddr(addr string, fname string, dt time.Duration) error {
	conn, e := Connect(addr, dt)
	if e != nil {
		return e
	}
	e = GuaranteedWriteFile(conn, nil, fname, dt)
	return e
}

func GuaranteedWriteFileToAddrWithPre(addr string, pre []byte, fname string, dt time.Duration) error {
	conn, e := Connect(addr, dt)
	if e != nil {
		return e
	}
	conn.Write(append(pre, 0x11))
	e = GuaranteedWriteFile(conn, nil, fname, dt)
	return e
}

func GuaranteedRead(conn net.Conn, r *bufio.Reader, dt time.Duration) ([]byte, error) {
	t0 := time.Now()
BEG:
	s, e := r.ReadBytes(0x11) //  this is not buffered!
	if e != nil {
		log.Println("utils GuaranteedRead error reading ", e)
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG

		} else {
			return make([]byte, 0), errors.New("utils: read string")
		}
	}
	if len(s) < 3 {
		log.Println("utils GuaranteedRead very short string ")
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return make([]byte, 0), errors.New("very short string")
		}
	}
	n0 := int(s[0])
	n1 := int(s[1])
	n2 := int(s[2])
	n := n0*256*256 + n1*256 + n2
	if len(s)-3 < n {
		log.Println("utils GuaranteedRead short string ")
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return make([]byte, 0), errors.New("short string")
		}
	}
	conn.Write(append([]byte("success"), 0x11))
	return s[3 : len(s)-1], nil // chop off the 0x11
}

func GuaranteedReadBuffered(conn net.Conn, r *bufio.Reader, dt time.Duration) ([]byte, error) {
	t0 := time.Now()

	//buf := bufio.NewReader(conn)

BEG:
	s, e := r.ReadBytes(0x11) //  this is not buffered!
	if e != nil {
		log.Println("utils GuaranteedRead error reading ", e)
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG

		} else {
			return make([]byte, 0), errors.New("utils: read string")
		}
	}
	if len(s) < 3 {
		log.Println("utils GuaranteedRead very short string ")
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return make([]byte, 0), errors.New("very short string")
		}
	}
	n0 := int(s[0])
	n1 := int(s[1])
	n2 := int(s[2])
	n := n0*256*256 + n1*256 + n2
	if len(s)-3 < n {
		log.Println("utils GuaranteedRead short string ")
		conn.Write(append([]byte("unsuccess"), 0x11))
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			goto BEG
		} else {
			return make([]byte, 0), errors.New("short string")
		}
	}
	conn.Write(append([]byte("success"), 0x11))
	return s[3 : len(s)-1], nil // chop off the 0x11
}

func GuaranteedSend(addr string, x []byte, dt time.Duration) error {
	t0 := time.Now()
BEG:
	dialer := net.Dialer{Timeout: 30 * time.Second}
	conn, e := dialer.Dial("tcp", addr)
	if e != nil {
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			time.Sleep(time.Second)
			goto BEG
		} else {
			return errors.New("cannot connect")
		}
	}
	r := bufio.NewReader(conn)
	return GuaranteedWrite(conn, r, x, dt)
}

func GuaranteedSendFileWithPre(addr string, pre []byte, fname string, dt time.Duration) error {
	t0 := time.Now()
BEG:
	dialer := net.Dialer{Timeout: 30 * time.Second}
	conn, e := dialer.Dial("tcp", addr)
	if e != nil {
		t := time.Now()
		if t.Before(t0.Add(dt)) {
			time.Sleep(time.Second)
			goto BEG
		} else {
			return errors.New("cannot connect")
		}
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	conn.Write(pre)
	x, e := ioutil.ReadFile(fname)
	if e != nil {
		return e
	}
	e1 := GuaranteedWrite(conn, r, x, dt)
	return e1
}

/*
func LastStringIndex(f * os.File, s string)int{

	if f == nil{
		return -1
	}
	fi,_ := f.Stat()

	n := fi.Size()

	b := make([]byte, 4096)

	f.Seek


}
*/
