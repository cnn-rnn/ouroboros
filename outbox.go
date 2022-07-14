package main

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	cluster "ouroboros/src/cluster"
	"ouroboros/src/constants"
	"ouroboros/src/load_conf"
	node "ouroboros/src/node"
	utils "ouroboros/src/utils"

	"github.com/pkg/profile"
)

var H = 64

type Conf struct {
	Dir0      string
	Me        node.Node
	Mes       []byte
	Next      map[string][]string
	Nodes     map[string]node.Node
	Immediate map[string]string
	PeerState map[string]string
	mx        sync.Mutex
}

var conf Conf

var xx cluster.Cluster

var O = make(chan string, 10000)
var WC = make(map[string]chan string)
var TMP = make(map[string]chan string)

//var Cou = make(map[string]int)

var Cou = make(map[string]int)

var cm sync.Mutex

var nworkers = 0

var NUM = 0
var COUG = 0

func Configure(dname string) {

	conf.Dir0 = dname
	load_conf.LoadWaitInterface(dname+"/conf/me.txt", &conf.Me)
	load_conf.LoadWaitInterface(dname+"/net/nodes.txt", &conf.Nodes)
	load_conf.LoadWaitInterface(dname+"/net/routing.txt", &conf.Next)
	load_conf.LoadWaitInterface(dname+"/net/cluster.txt", &xx)

	xx.Rou.SetPointer(&xx.Nodes)
	xx.RT.SetPointer(&xx.Nodes)

	log.Println("xx=", xx.Strong())

	if conf.Next == nil {
		conf.Next = make(map[string][]string)
	}
	conf.Next[conf.Me.Id] = []string{conf.Me.Id}
	log.Println(conf)

	conf.Immediate = make(map[string]string)

	log.Println("immediate neighbors")
	for i := range conf.Next {
		for j := range conf.Next[i] {
			n := conf.Next[i][j]
			conf.Immediate[n] = n
		}
	}

	conf.PeerState = make(map[string]string)

	conf.Mes, _ = json.Marshal(conf.Me)

	s, e := ioutil.ReadFile(conf.Dir0 + "/outbox_data/ostat.txt")
	B := true
	if e == nil {
		e = json.Unmarshal(s, &Cou)
		if e == nil {
			B = false
		}
	}
	if B {
		Cou = make(map[string]int)
		for id := range xx.Nodes {
			Cou[id] = 0
		}
	} else {
		for id := range xx.Nodes {
			if _, ok := Cou[id]; !ok {
				Cou[id] = 0
			}
		}
	}

}

//haha

func worker(node_id string, o chan string) {

	nworkers += 1

	node := conf.Nodes[node_id]
	b := make([]byte, 4096)
	COU := 0
BEG:
	dialer := net.Dialer{Timeout: 30 * time.Second}
	//conn, e := dialer.Dial("tcp", "localhost:"+node.Port_p)
	conn, e := dialer.Dial("tcp", node.Ip+":"+node.Port_p)
	if e != nil {
		time.Sleep(time.Second)

		log.Println(node_id, "cannot connect", e)
		log.Println("emptying channel")
		for {
			fn := <-WC[node_id]

			log.Println(fn, len(WC[node_id]), len(WC[node_id]))
			//os.Rename(conf.Dir0+"/oubtbox/"+fn, conf.Dir0+"/inactive/"+fn)
			os.Remove(conf.Dir0 + "/oubtbox/" + fn)
			if len(WC[node_id]) < 3 {
				log.Println("going to BEG")
				goto BEG
			}
		}
	}

	rea := bufio.NewReader(conn)

	log.Println("worker connected to ", node_id)

	for {
		//fn := <-c

		var fn string
		fn = <-WC[node_id]

		f, e := os.OpenFile(conf.Dir0+"/outbox1/"+fn, os.O_RDWR, 0644)
		if e != nil {
			log.Println("oubox error opening file", e)
			continue
		}
		x, e3 := syscall.Mmap(int(f.Fd()), 0, 12+64+64+64, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if e3 != nil {
			log.Println("e3", e3)
			continue
		}

		msg_id := make([]byte, 64)
		dest_id := make([]byte, 64)
		copy(msg_id, x[12+64:12+64+64])
		copy(dest_id, x[12+64+64:12+64+64+64])
		syscall.Munmap(x)
		buf := bufio.NewReader(f)
		c := 0
		cc := 0
		c1 := 0
	MID:
		for {
			n, e1 := buf.Read(b)

			//log.Println(string(b))

			c1 += 1
			if e1 != nil {
				if e1 != io.EOF {
					log.Println("outbox: error reading from file", e1, "c1=", c1, "COU", COU)
					f.Close()
					o <- fn
					break
				}
			}

			cc += 1
			B := false
			for i := 0; i < n; i++ {
				if b[i] == 0x11 {
					log.Println("tebe fofan:", fn)
					B = true
				}
				if B {

					log.Println("Btrue closing")

					f.Close()
					os.Rename(conf.Dir0+"/outbox1/"+fn, conf.Dir0+"/fofan/"+fn)
					break
				}
			}

			m, e2 := conn.Write(b[0:n])
			if e2 != nil || m < n {
				log.Println("outbox could not complete write", n, m, e2)
				f.Close()
				conn.Close()
				o <- fn
				goto BEG
			}
			if e1 == io.EOF {

				//				log.Println("EOF c1", c1)

				conn.Write([]byte{0x11})
				response0, e4 := rea.ReadBytes(0x11)
				response := string(response0)
				if e4 != nil {
					log.Println("cannot read reponse", e4)
					conn.Close()
					f.Close()
					goto BEG
				}
				if len(response) <= 1 {
					log.Println("MID", response)
					f.Seek(0, 0)
					goto MID
				}
				response = response[0 : len(response)-1]
				if response != "success" {
					f.Seek(0, 0)
					log.Println("transfer uncsuccessful, repeating", fn)
					c += 1
					if c < 3 {

						log.Println("going to MID")

						goto MID
					} else {

						log.Println("three attmpts made node may be down", node_id, ",or\nfile is corrupt", fn, "probably \n\n\n\n")
					}
				}

				//				log.Println("success", node_id, fn)

				f.Close()
				os.Remove(conf.Dir0 + "/outbox1/" + fn)
				COU += 1
				if COU%1000 == 0 {
					log.Println("node_id", node_id, "COU", COU)
				}
				break
			}

		}
	}
}

func List(to string, dir string) {
	f, _ := os.OpenFile(to, os.O_CREATE|os.O_WRONLY, 0644)
	cmd := exec.Command("ls", dir)
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	cmd.Run()
	f.Close()
}

func SendToChan(dest_id_s string, fn string) {
	if WC[dest_id_s] == nil {
		log.Println("spinning another worker")
		WC[dest_id_s] = make(chan string, 100)
		go worker(dest_id_s, O)
		WC[dest_id_s] <- fn
	} else {
		log.Println("WC exists, len=", len(WC[dest_id_s]), dest_id_s, "status", conf.Nodes[dest_id_s].Status)
		WC[dest_id_s] <- fn
	}

}

func manager() {
	fs, e := ioutil.ReadDir(conf.Dir0 + "/outbox/")
	utils.Check(e, "outbox error")

	List(conf.Dir0+"/outbox_fs.txt", conf.Dir0+"/outbox")

	fl, e := os.OpenFile(conf.Dir0+"/outbox_fs.txt", os.O_RDONLY, 0644)
	if e != nil {
		log.Println("no outbox_fs fiel")
		time.Sleep(time.Second)
		return
	}

	buf := bufio.NewReader(fl)

	c := 0
	wr := 0
	for {
		c += 1

		fn, e := buf.ReadString('\n')

		if e != nil {
			break
		}
		fn = fn[:len(fn)-1]
		f, e := os.OpenFile(conf.Dir0+"/outbox/"+fn, os.O_RDWR, 066)
		if e != nil {
			//			log.Println("outbox manager ", e, fn)
			wr += 1
			continue
		}

		COUG += 1

		fd := int(f.Fd())
		N1 := 12 + 64 + H + H + H
		x, e1 := syscall.Mmap(fd, 0, int(N1), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if e != nil {
			log.Fatal("outbox manager mmap", e1)
		}

		if len(x) < N1 {
			log.Fatal("short x", len(x), conf.Dir0+"/outbox/"+fn)
		}
		dest_id := x[12+64+H : 12+64+H+H]
		dest_id_s := string(dest_id)
		if dest_id_s == conf.Me.Id {
			log.Println("send files to myself")
		}

		Cou[dest_id_s] += 1

		syscall.Munmap(x)
		f.Close()

		if xx.Nodes[dest_id_s].Status != constants.Active {
			//log.Println("dest_id iactive", dest_id_s)
			os.Rename(conf.Dir0+"/outbox/"+fn, conf.Dir0+"/inactive/"+fn)
			continue
		}

		ids := conf.Next[dest_id_s]
		if len(ids) == 0 {
			//os.Rename(conf.Dir0+"/outbox/"+fn, conf.Dir0+"/no_peers/"+fn)
			log.Println("no peers, removing", fn)
			os.Remove(conf.Dir0 + "/outbox/" + fn)
			continue
		}
		next_id := ids[0]
		var B0 bool
		conf.mx.Lock()
		//B0 = conf.PeerState[next_id] != "down"

		B0 = xx.Nodes[dest_id_s].Status == constants.Active

		conf.mx.Unlock()
		if B0 {
			os.Rename(conf.Dir0+"/outbox/"+fn, conf.Dir0+"/outbox1/"+fn)

			if WC[next_id] == nil {
				log.Println("WC ", next_id, nil)
			}

			WC[next_id] <- fn

			//SendToChan(next_id, fn)
		} else {
			i := 1
			conf.mx.Lock()
			for ; i < len(ids) && xx.Nodes[ids[i]].Status != constants.Active; i++ {
			}
			conf.mx.Unlock()
			if i < len(ids) {
				peer := ids[i]
				os.Rename(conf.Dir0+"/outbox/"+fn, conf.Dir0+"/outbox1/"+fn)

				if WC[peer] == nil {
					log.Println("WC ", next_id, nil)
				}

				WC[peer] <- fn

				//SendToChan(peer, fn)
			} else {
				log.Println("do not attempt direct send")
				//SendToChan(dest_id_s, fn)
				os.Rename(conf.Dir0+"/outbox/"+fn, conf.Dir0+"/unreachable/"+fn)
				continue
			}
		}
	}
	if c == 0 && len(fs) > 0 {
		B := true
		conf.mx.Lock()
		for i := range conf.PeerState {
			if conf.PeerState[i] != "down" {
				B = false
			}
		}
		conf.mx.Unlock()
		if B {
			log.Println("all peers are down, waiting")
			time.Sleep(time.Second)
		}
	}

	utils.MakeDir0(conf.Dir0 + "/outbox_old_fs")
	os.Rename(conf.Dir0+"/outbox_fs.txt", conf.Dir0+"/outbox_old_fs/"+strconv.FormatInt(time.Now().UnixNano(), 10))

	log.Println("processed", c, "files wr", wr, "nworkers=", nworkers)

	s, e := json.Marshal(Cou)
	if e == nil {
		ioutil.WriteFile(conf.Dir0+"/outbox_data/ostat.txt", s, 0644)
	}

}

func handleUnsentFiles() {
	utils.MakeDir0(conf.Dir0 + "/unsent_links")
	for fn := range O {
		log.Println("handle unsent file", fn)
		os.Rename(conf.Dir0+"/outbox1/"+fn, conf.Dir0+"/unsent_links/"+fn)
	}
}

func createWorkers() {
	for i := range conf.Immediate {
		WC[i] = make(chan string, 1000)
		go worker(i, O)
	}
}

func gracefulExit() {

	s, e := json.Marshal(Cou)
	if e == nil {
		ioutil.WriteFile(conf.Dir0+"/outbox_data/ostat.txt", s, 0644)
	}

	for {
		B := false
		for id := range WC {
			if len(WC[id]) > 0 {
				log.Println("exit: there are unprocessed files in channel ", id, len(WC[id]))
				B = true
			}
		}
		if B {
			log.Println("wating 10 sec")
			time.Sleep(10 * time.Second)
		} else {
			log.Println("safe to exit")
			break
		}
	}
	os.Exit(0)
}

func main() {
	defer profile.Start().Stop()

	var dir0 string
	if len(os.Args) > 1 {
		dir0 = os.Args[1]
	} else {
		dir0 = "/d"
	}
	conf.Dir0 = dir0

	utils.MakeDir0(conf.Dir0 + "/outbox_data")

	utils.MakeDir0(conf.Dir0 + "/fofan")
	utils.MakeDir0(conf.Dir0 + "/no_peers")
	utils.MakeDir0(conf.Dir0 + "/unreachable")
	utils.MakeDir0(conf.Dir0 + "/inactive")

	Configure(dir0)

	createWorkers()

	go handleUnsentFiles()

	s, e := ioutil.ReadFile(conf.Dir0 + "/outbox_data/NUM.txt")
	log.Println("initial COU", s)
	if e == nil {
		c, e := strconv.Atoi(string(s))
		if e == nil {
			NUM = c
		} else {
			NUM = 0
		}
	} else {
		NUM = 0
	}

	var t0 int64

	f, e := os.OpenFile(conf.Dir0+"/net/cluster.txt", os.O_RDONLY, 0644)
	if e != nil {
		log.Println("no cluster file, exitting")
		time.Sleep(30 * time.Second)
	}
	fi, _ := f.Stat()
	t0 = fi.ModTime().UnixNano()
	f.Close()

	for {

		f, e := os.OpenFile(conf.Dir0+"/net/cluster.txt", os.O_RDONLY, 0644)
		if e != nil {
			log.Println("no cluster file, exitting")
			time.Sleep(30 * time.Second)
		}
		fi, _ := f.Stat()
		t := fi.ModTime().UnixNano()
		f.Close()
		if t != t0 {
			log.Println("cluster configurattion modified. restarting service.")

			ioutil.WriteFile(conf.Dir0+"/outbox_data/NUM.txt", []byte(strconv.Itoa(NUM+1)), 0644)

			gracefulExit()
		}

		manager()

		time.Sleep(30 * time.Second)

	}
}
