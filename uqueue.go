package main

// this process maintains queue of urls to fetch

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	btr "ouroboros/src/btr"
	utils "ouroboros/src/utils"
)

var prefix = string(0x11) + string(0x11) + string(0x11) + string(0x11) + string(0x11) // 5 symbols, see btma_hash0

type Conf struct {
	Dir0 string
}

var conf Conf

var rs = rand.NewSource(time.Now().UnixNano())
var ra = rand.New(rs)

var hchan = make(chan string, 100)
var uchan = make(chan string, 100)
var inactive = make(chan string, 100)

var ina = make(map[string]bool)

var il sync.Mutex

var symbols []int

var P = 4

func activate(buf *bufio.Reader, y *btr.Tree, ep int64, poi int64, many int64) (int64, bool) {
	log.Println("----------------activating-------------------- ep=", ep)
	cou := int64(0)
	B := false
	for ep < poi && cou < many {
		s, e := buf.ReadString('\n')
		ep += int64(len(s))
		cou += int64(len(s))
		if e != nil {
			B = true
			break
		}
		i := strings.Index(s, prefix)
		if i < 0 {
			//			log.Println("prefix not found", s, []byte(s))
			continue
		}
		if i+P+1 >= len(s) {
			log.Println("short+index", s, []byte(s))
			continue
		}
		if s[i+P+1] != '+' {
			//			log.Println("inactive", s[i:len(s)-1], []byte(s), byte(s[i+P+1]))
			if s[i+P+1] != '-' {
				log.Println("nonstandard", s)
			}
			continue
		}
		s1 := s[i+P+1+1 : len(s)-1]
		t := y.Find(s1)
		if t != nil {
			//			log.Println("setting active", s1)
			y.SetActive(s1)
		} else {
			//			log.Println("Adding", s1)
			y.Add(s1)
		}
	}

	log.Println("activation done -----------------------ep=", ep)

	return ep, B
}

// this function loads host list in the segment [i][j]  ( so the hostname = [i][j]+rest) and draws a host randomly from it.
//then it calls DrawfromHost function, and attempts to draw a url from that host.
// it successfull, it adds url to the queue,
// if not, it mars host inactive.
// If host is marked as inactive, it will not participate in the further Draws in this session.
// We make at most 100 attmpts to draw from given segment. Then we close and go to the next segment.
// This approach does not have the problem of freshly coming urls, because these will be addressed on next iteration.

// there Is potential problem, when most of the hosts are exhausted: then , we just spend most of the time to make attempts on hosts that are already
// exhausted, and this leaves us little time to actually draw from 'active' hosts.

func DrawHost() {
	tot := 0
	y := btr.New()
	poi := int64(0)

	ep := int64(0)

	for {

		log.Println("reloading hosts \n\n\n\n")

		//		lfile, e := os.OpenFile(conf.Dir0+"/host_list.txt", os.O_RDONLY, 0644)
		lfile, e := os.OpenFile(conf.Dir0+"/hostsd/host.txt", os.O_RDONLY, 0644)
		if e != nil {
			log.Println("cannot open hostlist")
			time.Sleep(time.Second)
			continue
		}
		buf := bufio.NewReader(lfile)

		pois, e := ioutil.ReadFile(conf.Dir0 + "/uqueue.poi")
		if e == nil {
			poi1, e := strconv.ParseInt(string(pois), 10, 64)
			if e == nil {
				poi = poi1
			}
		}

		lfile.Seek(ep, 0)
		var B bool
		ep, B = activate(buf, y, ep, poi, int64(100000))
		if B {
			log.Println("-------------------end reached,", ep, poi)
			ep = 0
		} else {
			log.Println(" activated ep=", ep)
		}

		lfile.Seek(poi, 0)
		q := make([]string, 0)
		log.Println("---------------------addingn new------------------- ", " U=", y.GetU())
		for {
			s, e := buf.ReadString('\n')
			poi += int64(len(s))
			if e != nil {
				break
			}
			i := strings.Index(s, prefix)
			if i < 0 {
				log.Println("prefix not found", s)
				continue
			}
			if i+P+1 >= len(s) {
				log.Println("short+index", s)
				continue
			}
			if s[i+P+1] != '+' {
				//				log.Println("inactive", s[i:len(s)-1])
				if s[i+P+1] != '-' {
					log.Println("nonstandard", s)
				}
			}
			s1 := s[i+P+1 : len(s)-1]
			q = append(q, s1)
		}

		ioutil.WriteFile(conf.Dir0+"/uqueue.poi", []byte(strconv.FormatInt(poi, 10)), 0644)

		log.Println("------------------------------------------------got hosts as list", len(q))

		if len(q) == 0 {
			log.Println("short host file")
			//time.Sleep(time.Duration(1e+9))
			//continue
		}

		y = y.Add("google.com")
		for i := 0; i < len(q); i++ {
			y.Add(q[i])
		}

		log.Println("-----------------------------------------------------y refreshed", y.GetU())
		time.Sleep(time.Second)

		COU := 0
		for COU < 100000 && y.GetU() > 0 {

			cou := 0
			real := 0
			for real < 1000 && cou < 100000 && y.GetU() > 0 {
				host, e := y.Draw(ra)
				if e != nil {
					log.Println("breaking", host)
					y.SetInactive(host)
					time.Sleep(time.Duration(1e+9))
					break
				}
				k := DrawFromHost(host)
				if k < 0 {
					y.SetInactive(host)
				} else {
					real += 1
					COU += 1
					tot += 1
				}
				cou += 1
			}
			log.Println("drawhost", "cou", cou, "real", real, "y.GetU", y.GetU(), "COU", COU, "tot", tot)
		}
		log.Println("sleeping 1sec")
		time.Sleep(time.Second)
	}
}

//maybe draw ~10 from single active host ?

//what about WG and Harmonic centrality here?

// I dont even need to care about activity of the host: I check only once in a batch;  it has negligible cost

func DrawFromHost(host string) int {
	h, e := os.OpenFile(conf.Dir0+"/resou/"+host, os.O_RDONLY, 0644)
	if e != nil {
		return 1
	}
	hi, _ := h.Stat()
	s1 := int(hi.Size())
	s2, e := ioutil.ReadFile(conf.Dir0 + "/resou/" + host + ".poi")
	s3 := 0
	if e != nil || len(s2) <= 0 {
	} else {
		var e1 error
		s3, e1 = strconv.Atoi(string(s2))
		if e1 != nil {
			if string(s2) != "" {
				h.Close()
				return -2
			}
		}
	}
	if s1 <= s3 {
		h.Close()
		return -3
	}
	h.Seek(int64(s3), io.SeekStart)
	b := bufio.NewReader(h)
	s, e := b.ReadString('\n')
	if e != nil {
		h.Close()
		return -4
	}
	if len(s) <= 1 {
		h.Close()
		return -5
	}
	s3 += len(s)
	ioutil.WriteFile(conf.Dir0+"/resou/"+host+".poi", []byte(strconv.Itoa(s3)), 0644)
	uchan <- string(s[0 : len(s)-1])
	h.Close()
	return 0
}

//perhaps keep hosts in RAM? how to decide?  calculation?

//need to reload host file: "refresh" --- because new hosts become available; You are doing it: on new .AsList() call you get fresh file

// in principle , can use a db: host -> activity_status;  and need not get hosts in RAM

func Run() {
	COU := 0
	for {
		f, e := os.OpenFile(conf.Dir0+"/queue.txt", os.O_CREATE|os.O_RDWR, 0644)
		if e != nil {
			return
		}
		fi, _ := f.Stat()
		f.Close()
		poi := 0
		ps, e := ioutil.ReadFile(conf.Dir0 + "/queue.txt.poi")
		if e != nil || len(ps) == 0 {
		} else {
			poi, e = strconv.Atoi(string(ps))
			if e != nil {
				time.Sleep(time.Second)
				continue
			}
		}
		//log.Println("diff", int(fi.Size())-poi, len(uchan))
		if int(fi.Size())-poi < 100000 {
			url := <-uchan
			COU += 1
			que, _ := os.OpenFile(conf.Dir0+"/queue.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			que.Write([]byte(url + "\n"))
			que.Close()

			//	log.Println("added", url)

		} else {

			log.Println("enough", int(fi.Size())-poi)

			time.Sleep(time.Second)
		}
	}
}

func main() {
	var dir0 string
	if len(os.Args) > 1 {
		dir0 = os.Args[1]
	} else {
		dir0 = "/d"
	}
	conf.Dir0 = dir0
	utils.MakeDir0(conf.Dir0 + "/hostsd")
	utils.MakeDir0(conf.Dir0 + "/host_msg")
	utils.MakeDir0(conf.Dir0 + "/host_msg_tmp")
	/*
		for i := 48; i < 58; i++ {
			symbols = append(symbols, i)
		}
		for i := 97; i < 123; i++ {
			symbols = append(symbols, i)
		}
	*/
	for i := 0; i < 256; i++ {
		symbols = append(symbols, i)
	}

	go DrawHost()
	Run()
}
