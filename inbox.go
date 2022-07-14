package main

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"ouroboros/src/load_conf"
	node "ouroboros/src/node"

	//	"ouroboros/src/types"
	utils "ouroboros/src/utils"
)

var H = 64

type Conf struct {
	me    node.Node
	Dir0  string
	Next  map[string][]string
	Nodes map[string]*node.Node
}

var conf Conf

var inb = 0

var dir0 string

var successes = make(map[int]int)
var failures = make(map[int]int)

var rs = rand.NewSource(time.Now().UnixNano())
var ra = rand.New(rs)

var Cou = make(map[string]int)
var mx sync.Mutex

func Config(dname string) {
	conf.Dir0 = dname

	load_conf.LoadWaitInterface(dname+"/conf/me.txt", &conf.me)
	load_conf.LoadWaitInterface(dname+"/net/nodes.txt", &conf.Nodes)
	load_conf.LoadWaitInterface(dname+"/net/routing.txt", &conf.Next)

	//logf, err := os.OpenFile(dname+"/logs/inbox", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	//utils.Check(err, "inbox Config")
	//log.SetOutput(logf)
	//log.Println("\n\n\n\n\n\n Congigured \n expecting conn from peers:")

	//	log.Println("here are the matrices")
	//	log.Println(conf.X)
	//	log.Println(conf.Y)

	s, e := ioutil.ReadFile(conf.Dir0 + "/inbox_data/istat.txt")
	if e == nil {
		e = json.Unmarshal(s, &Cou)
		if e == nil {
		}
	}

}

func ListenForPeers() {
	log.Println("listen for peers", ":"+conf.me.Port_p)
	ln, e := net.Listen("tcp", ":"+conf.me.Port_p)
	utils.Check(e, "listening error")
	for {
		conn, _ := ln.Accept()
		log.Println("inbox: got peer", conn.RemoteAddr())
		//go ReceiveFromPeer(conn)
		go receive(conn)
	}
}

func receive(conn net.Conn) {
	buf := bufio.NewReader(conn)
	//b := make([]byte, 4096)
	COU := 0
	T48 := 0
	T49 := 0
	tome := 0
	outside := 0
	for {

		s, e := buf.ReadBytes('\n') // 0x11

		if e != nil {
			log.Println("received error", e)
			conn.Close()
			return
		}

		n := len(s)
		if n < 12+64 || e != nil {
			conn.Write(append([]byte("short header"), 0x11))
			log.Println("short header", n, e)
			conn.Close()
			return
		}

		sha := string(s[12 : 12+64])
		_ = sha
		ty := s[10]
		msg_id := string(s[12+64 : 12+64+H])
		dest_id := string(s[12+64+H : 12+64+H+H])
		orig_id := string(s[12+64+H+H : 12+64+H+H+H])
		temp_id := utils.Generate_id(ra)

		s1, e := buf.ReadBytes(0x11)

		if dest_id != conf.me.Id {
			f, e1 := os.OpenFile(conf.Dir0+"/outbox/"+msg_id, os.O_CREATE|os.O_RDWR, 0644)
			if e1 != nil {
				log.Fatal("ass", e1)
			}
			f.Write(s)
			f.Write(s1[:len(s1)-1])
			f.Close()
			outside += 1
		} else {
			tome += 1

			mx.Lock()
			Cou[orig_id] += 1
			mx.Unlock()

			if ty == 48 {
				f, e1 := os.OpenFile(conf.Dir0+"/tmp_crawler_links1/"+msg_id, os.O_CREATE|os.O_RDWR, 0644)
				if e1 != nil {
					log.Fatal("ass", e1)
				}
				f.Write(s1[:len(s1)-1])
				f.Close()
				T48 += 1
			}
			if ty == 49 {
				f, e1 := os.OpenFile(conf.Dir0+"/inbox_ranker/"+msg_id, os.O_CREATE|os.O_RDWR, 0644)
				if e1 != nil {
					log.Fatal("ass", e1)
				}
				f.Write(s)
				f.Write(s1[:len(s1)-1])
				f.Close()
				T49 += 1
			}
			if ty != 48 && ty != 49 {
				log.Println("unknown type")
				os.Remove(conf.Dir0 + "/tmp/" + temp_id)
			}
		}
		conn.Write(append([]byte("success"), 0x11))
		if COU%1000 == 0 {
			log.Println("succcess COU", COU, "T48", T48, "T49", T49, "tome", tome, "outside", outside)

			s, e := json.Marshal(Cou)
			if e == nil {
				ioutil.WriteFile(conf.Dir0+"/inbox_data/istat.txt", s, 0644)
			}

		}
		COU += 1

	}
}

func Status() {
	for {
		s := "successes:\n"
		for i := range successes {
			s += strconv.Itoa(i) + "  :  " + strconv.Itoa(successes[i]) + "\n"
		}
		s += "\n\n\nfailures:\n"
		for i := range failures {
			s += strconv.Itoa(i) + "  :  " + strconv.Itoa(failures[i]) + "\n"
		}
		ioutil.WriteFile(conf.Dir0+"/logs/inbox.status", []byte(s), 0644)
		time.Sleep(60 * time.Second)
	}
}

func main() {
	if len(os.Args) > 1 {
		dir0 = os.Args[1]
	} else {
		dir0 = "/d"

		s, e := ioutil.ReadFile("/etc/dse/dse.conf")
		if e != nil {
			log.Println("error reding config", e)
			time.Sleep(time.Second)
			os.Exit(0)
		}
		q := strings.Split(string(s), "\n")
		dir0 = q[0]

		log.Println("dir0=", dir0)
	}

	Config(dir0)

	utils.MakeDir0(conf.Dir0 + "/inbox_data")

	go Status()
	ListenForPeers()
}
