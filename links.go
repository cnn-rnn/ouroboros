package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	"time"

	"ouroboros/src/load_conf"
	node "ouroboros/src/node"

	//"./src/utils"
	cluster "ouroboros/src/cluster"
	utils "ouroboros/src/utils"
)

var H = 64

/*
type Nodeb struct {
	Id     string
	Ip     string
	Port_p string

	I10 string
	I20 string
	I11 string
	I21 string
	I12 string
	I22 string

	N int
}
*/

type Conf struct {
	Dir0 string
	Me   node.Node
	//	Ln   int

	t0 int64
}

var conf Conf

var xx cluster.Cluster

var outChan = make(chan []byte, 100000)

var rs = rand.NewSource(time.Now().UnixNano())
var ra = rand.New(rs)

var Cou = make(map[string]int)

func Convert(s []byte) string {
	y := sha256.Sum256((s))
	id := hex.EncodeToString(y[:])
	return id
}

func Worker() {

	f0, e0 := os.OpenFile(conf.Dir0+"/net/cluster.txt", os.O_RDONLY, 0644)
	if e0 != nil {
		log.Println("no cluster file")
		time.Sleep(time.Second)
		os.Exit(0)
	}
	fi, _ := f0.Stat()
	t0 := fi.ModTime().UnixNano()
	f0.Close()

	Counter := make(map[string]int)
	COU := 0

	for {

		f0, e0 = os.OpenFile(conf.Dir0+"/net/cluster.txt", os.O_RDONLY, 0644)
		fi, _ = f0.Stat()
		t := fi.ModTime().UnixNano()
		f0.Close()
		if t != t0 {
			log.Println("cluster configuration changed, reloading service")
			os.Exit(0)
		}

		fs, e := ioutil.ReadDir(conf.Dir0 + "/tmp_links")
		log.Println("\nlenfs", len(fs), "e", e)
		if e != nil || len(fs) == 0 {
			time.Sleep(time.Second)
			continue
		}
		for j := range fs {
			fn := fs[j].Name()

			f, _ := os.OpenFile(conf.Dir0+"/tmp_links/"+fn, os.O_RDONLY, 0644)
			buf := bufio.NewReader(f)
			for {
				s, e := buf.ReadBytes('\n')
				if e != nil {
					f.Close()
					os.Remove(conf.Dir0 + "/tmp_links/" + fn)
					break
				}
				if len(s) <= 1 {
					continue
				}
				if s[len(s)-1] != '\n' {
					log.Fatal("obvious check failed", s)
				}
				doc_id := utils.Url_Id(string(s[0 : len(s)-1]))
				ids := xx.Doc_Peer(doc_id)

				//log.Print(len(ids), " ")

				for ii := range ids {
					node_id := ids[ii]

					Cou[node_id] += 1

					if node_id == "" {
						log.Println("empty node_id, len(node_ids)", len(ids))
						continue
					}

					if node_id == conf.Me.Id {
						log.Println("not sending to myself")
						continue
					}

					if _, err := os.Stat(conf.Dir0 + "/tmp_crawler/" + node_id); err != nil {

						msg_id := utils.Generate_id_long(ra, 64)

						p := make([]byte, 12)
						p[10] = 48

						p = append(p, make([]byte, 64)...)

						p = append(p, []byte(msg_id)...)

						p = append(p, []byte(node_id)...)
						p = append(p, []byte(conf.Me.Id)...)
						p = append(p, make([]byte, 9*H)...)
						p = append(p, '\n')

						//log.Println("initializing", conf.Dir0+"/tmp_crawler/"+node_id)

						ioutil.WriteFile(conf.Dir0+"/tmp_crawler/"+node_id, p, 0644)
					}

					F, e := os.OpenFile(conf.Dir0+"/tmp_crawler/"+node_id, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
					//log.Println("F", F, "e", e)
					if e != nil {
						log.Fatal("eee", e)
					}

					F.Write(s)

					fi, _ := F.Stat()

					if fi.Size() > 4096 {

						n := fi.Size()

						ns := strconv.Itoa(int(n))

						p0 := make([]byte, 10)

						if len(ns) > 10 {
							log.Fatal("file too long")
						}

						for j := range ns {
							p0[j] = ns[j]
						}

						x, _ := syscall.Mmap(int(F.Fd()), 0, 12+64+64, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

						for i := 0; i < len(p0); i++ {
							x[i] = p0[i]
						}

						hasher := sha256.New()
						F.Seek(0, 0)

						io.Copy(hasher, F)
						y := hex.EncodeToString(hasher.Sum(nil))

						for i := range y {
							x[12+i] = y[i]
						}

						msg_id0 := make([]byte, 64)
						copy(msg_id0, x[12+64:12+64+64])
						syscall.Munmap(x)
						F.Close()
						Counter[node_id] += 1
						COU += 1
						os.Rename(conf.Dir0+"/tmp_crawler/"+node_id, conf.Dir0+"/outbox/"+string(msg_id0))
						continue
					}
					F.Close()
				}
			}
		}
		time.Sleep(time.Second)

		s, e := json.Marshal(Cou)
		ioutil.WriteFile(conf.Dir0+"/links_data/lstat.txt", s, 0644)
	}
}

func Configure() {

	load_conf.LoadWaitInterface(conf.Dir0+"/conf/me.txt", &conf.Me)
	load_conf.LoadWaitInterface(conf.Dir0+"/net/cluster.txt", &xx)

	xx.Rou.SetPointer(&xx.Nodes)
	xx.RT.SetPointer(&xx.Nodes)

	log.Println("me =", conf.Me)
	log.Println("x =", xx.Strong())

	f, e := os.OpenFile(conf.Dir0+"/net/cover.txt", os.O_RDONLY, 0644)
	if e != nil {
		log.Fatal("out link manager")
	}
	fi, _ := f.Stat()
	conf.t0 = fi.ModTime().UnixNano()
	f.Close()

	if H != len(conf.Me.Id) {
		panic("H != len Id=" + conf.Me.Id)
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

	Configure()

	utils.MakeDir0(conf.Dir0 + "/links_data")
	utils.MakeDir0(conf.Dir0 + "/tmp_links")
	utils.MakeDir0(conf.Dir0 + "/tmp_links1")

	Worker()

}
