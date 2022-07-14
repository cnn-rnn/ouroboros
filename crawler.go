package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	fetcher "ouroboros/src/fetcher"
	"ouroboros/src/load_conf"
	node "ouroboros/src/node"
	proc "ouroboros/src/proc"
	"ouroboros/src/seed"

	//"ouroboros/src/types"

	//	"./src/utils"
	utils "ouroboros/src/utils"

	"github.com/pkg/profile"
)

type Conf struct {
	Id       string
	I1       string
	I2       string
	N        int
	NWorkers int
	X        [][]int
	Xcompr   [][]int
	Y        [][][]int
	//	PeerMap     []types.Data0
	Dir0        string
	ProfilePort string
}

type Res struct {
	name  string
	links map[string]bool
}

type Resr struct {
	url string
	err error
}

type Wait struct {
	T int64
	X []string
}

var conf Conf

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var QueueRead *os.File
var QueueWrite *os.File
var QueuePoi *os.File

var Submitted *os.File
var Crawled *os.File
var Records *os.File

var N int
var R1 int
var R2 int

var W = make(map[string]*Wait)

var nResults = 100000
var nJobs = 10000
var nWorkers = 300

var hchan = make(chan HostState, 100000)
var wchan = make(chan string, 100000)
var results = make(chan Res, 2000)
var jobs = make(chan string, 10000)
var fchan = make(chan string, 100000)

var crawled = make(chan Resr, 100000)

var OutgoingLinks = make(chan string, 10000)

//var C = make([]chan []byte, 0)

var out = make(chan []byte, 100000)

var COU = int64(0)
var BR = int64(0)
var NProceccedURLs = 0
var ActiveWThreads = 0

var mxa sync.Mutex
var mxn sync.Mutex
var mxp sync.Mutex
var mxv sync.Mutex
var tlock sync.Mutex
var mxv0 sync.Mutex
var mxwriters sync.Mutex

var mi = 0
var mi0 = 0
var inb = 0

var T2 = int64(0)
var T3 = int64(0)

var rs = rand.NewSource(time.Now().UnixNano())
var ra = rand.New(rs)

var t0 = time.Now().UnixNano()
var tick = time.Now().UnixNano()

var Usa = float64(0)
var Nus = int64(0)

var couH = 0
var couQ = 0
var Act = 0
var Ina = 0
var ADD = 0

var Pointer = 0
var NWriters = 0

type Mc struct {
	Id     int
	C      chan string
	Status string
}

var mc = make([]Mc, 0)

type HostState struct {
	Host  string
	State string
}

func (w *Wait) Add(s string) *Wait {
	if w == nil {
		w = &Wait{0, make([]string, 0)}
	}
	w.T = time.Now().UnixNano()
	w.X = append(w.X, s)
	return w
}

func worker(id int) {

	var tr = &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    5 * time.Second,
		DisableCompression: true,
		DisableKeepAlives:  true,
	}
	var client = &http.Client{
		Transport: tr,
		Timeout:   20 * time.Second,
	}

	for url := range jobs {
		doc_id := string(utils.Url_Id(url))
		links, err := fetcher.One(client, url, conf.Dir0+"/pages/"+doc_id)

		crawled <- Resr{url, err}

		if err == nil {
			results <- Res{url, links}

			/*
				ioutil.WriteFile(conf.Dir0+"/pages/"+doc_id+".meta", []byte(url), 0644)
				f, e := os.OpenFile(conf.Dir0+"/pages/"+doc_id+".outlinks", os.O_CREATE|os.O_WRONLY, 0644)
				if e != nil {
					log.Fatal("crawler cannot create file", e)
				}
				buf := bufio.NewWriter(f)
				for i := range links {
					if len(i) == 0 {
						continue
					}
					buf.Write([]byte(i + "\n"))
				}
				buf.Flush()
				f.Close()
			*/
		} else {
			//log.Println("there was an error", url, err)
		}

	}
}

func MainLog() {
	t1 := time.Now().UnixNano()
	ds := float64(COU) / float64(t1-t0) * float64(1e+9)
	//	br := float64(BR) / float64(t1-t0) * float64(1e+9)
	s := strconv.Itoa(os.Getpid()) + "\n" + strconv.Itoa(int(COU)) + "\nds= " + strconv.FormatFloat(ds, 'E', -1, 64) + "\ninb= " + strconv.Itoa(inb) +
		"\nW= " + strconv.Itoa(len(W)) + "\nhchan= " + strconv.Itoa(len(hchan)) + "\nwchan= " + strconv.Itoa(len(wchan)) + "\nresults= " + strconv.Itoa(len(results)) +
		"\njobs= " + strconv.Itoa(len(jobs)) + "\ndr= " + strconv.Itoa(R2-R1)
	ioutil.WriteFile(conf.Dir0+"/logs/main.status", []byte(s), 0644)

	usa := proc.Usage(os.Getpid()) / float64(time.Now().UnixNano()-t0) * 1e+9

	fi, _ := QueueRead.Stat()
	log.Println(os.Getpid(), "    ", COU, "ds=", ds, "inb", inb, "W", len(W), "hchan", len(hchan), "wchan", len(wchan),
		"results=", len(results), "jobs=", len(jobs), "dr", R2-R1, "Usa", usa, "len(mc)", len(mc), "QueueSize", fi.Size(), "Pointer", Pointer,
		"couH", couH, "Act", Act, "Ina", Ina, "ADD", ADD, "NWriters", NWriters, "OutgoingLinks", len(OutgoingLinks))

	ioutil.WriteFile(conf.Dir0+"/COU.txt", []byte(strconv.Itoa(int(COU))), 0644)

}

func ProcessResults() {
	c := 0
	var tmp *os.File
	var tmp_id string
	var e error

	//myurl := 0
	for {

		if tmp == nil {
			tmp_id = utils.Generate_id(ra)
			tmp, e = os.OpenFile(conf.Dir0+"/tmp_crawler_links/"+tmp_id, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if e != nil {
				log.Println("cannot open file", e)
				time.Sleep(time.Second)
				continue
			}
		}
		COU += 1
		RE := <-results
		z := RE.links

		if COU%100 == 0 {
			MainLog()
		}

		//crawled <- RE.name

		//Crawled.Write(append([]byte(RE.name), '\n'))
		for x0 := range z {
			x := []byte(x0)
			w := IsMine(x)
			if w {
				//wchan <- x0

				tmp.Write([]byte(x0 + "\n"))
				c += len(x0)
			} else {
				OutgoingLinks <- x0
			}
		}

		if c > 8*1024 {
			tmp.Close()

			name := tmp_id
		BEG:
			name = utils.Generate_id(ra)
			if _, err := os.Stat(conf.Dir0 + "/tmp_crawler_links1/" + name); err == nil {
				goto BEG
			}

			os.Rename(conf.Dir0+"/tmp_crawler_links/"+tmp_id, conf.Dir0+"/tmp_crawler_links1/"+name)
			tmp = nil
			c = 0
		}
	}
}

func Recorder() {
	utils.MakeDir0(conf.Dir0 + "/fetch_results")
	for {
		s := <-crawled
		if s.err == nil {
			Crawled.Write(append([]byte(s.url), '\n'))
		}
		Records.Write([]byte(s.url + " " + fmt.Sprintln(s.err)))
		/*
			doc_id := string(utils.Url_Id(s.url))

			f, _ := os.OpenFile(conf.Dir0+"/fetch_results/"+doc_id+".result", os.O_CREATE|os.O_APPEND, 0644)

			//log.Println(s.url + " " + fmt.Sprintln(s.err))

			f.Write([]byte(s.url + " " + fmt.Sprintln(s.err)))
			f.Close()
		*/
	}
}

func OutgoingMessages() {
	//id := utils.Generate_id_long(ra, 64)
	//f, _ := os.OpenFile(conf.Dir0+"/tmp_links1/"+id, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	//buf := bufio.NewWriter(f)
	// *bufio.Writer
	var id string
	var f *os.File
	var buf *bufio.Writer
	f = nil
	c := 0
	for {

		if f == nil {
			id = utils.Generate_id_long(ra, 64)
			f, _ = os.OpenFile(conf.Dir0+"/tmp_links1/"+id, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			buf = bufio.NewWriter(f)
		}

		url := <-OutgoingLinks
		buf.Write([]byte(url + "\n"))
		c += len(url)
		if c > 10000 {
			buf.Flush()
			f.Close()
			f = nil
			buf = nil
			os.Rename(conf.Dir0+"/tmp_links1/"+id, conf.Dir0+"/tmp_links/"+id)
			c = 0
		}
	}
}

func AddJobs(id int) {

	r := bufio.NewReader(QueueRead)

	s0, _ := ioutil.ReadFile(conf.Dir0 + "/queue.txt.poi")
	Pointer, _ = strconv.Atoi(strings.TrimSpace(string(s0)))
	QueueRead.Seek(int64(Pointer), 0)

	for {

		fi, _ := QueueRead.Stat()
		D := fi.Size() - int64(Pointer)
		if D <= 512 {
			log.Println("D=", D, "jobs", len(jobs))
			n := ioutil.WriteFile(conf.Dir0+"/queue.txt.poi", []byte(strconv.Itoa(Pointer)), 0644)
			log.Println("RR", Pointer, []byte(strconv.Itoa(Pointer)), "n=", n)
			time.Sleep(time.Second)
			continue
		}

		x, e := r.ReadBytes('\n')
		if e != nil {
			log.Println("AddJobs reading e=", e)
			time.Sleep(time.Second)
			continue
		}

		Pointer += len(x)
		if ADD%500 == 0 {
			n := ioutil.WriteFile(conf.Dir0+"/queue.txt.poi", []byte(strconv.Itoa(Pointer)), 0644)
			log.Println("addjobs RR", Pointer, []byte(strconv.Itoa(Pointer)), "n=", n)
		}

		Submitted.Write(x)
		url := string(x[0 : len(x)-1])
		ADD += 1
		jobs <- url

	}
}

/*
func Lookup(x []byte) int {
	s := Convert(x)
	for i := range conf.PeerMap {
		if conf.PeerMap[i].I1 <= s && (s < conf.PeerMap[i].I2 || len(s) < len(conf.PeerMap[i].I2)) {
			return i
		}
	}
	return -1
}
*/
func Convert(s []byte) string {
	y := sha256.Sum256((s))
	id := hex.EncodeToString(y[:])
	return id
}

func IsMine(url []byte) bool {
	id := Convert(url)
	if conf.I1 <= id && id < conf.I2 {
		return true
	} else {
		return false
	}
}

func DeleteLocks() {
	ff, _ := ioutil.ReadDir(conf.Dir0 + "/hosts")
	for _, f := range ff {
		s := f.Name()
		if len(s) >= 5 && s[len(s)-5:len(s)] == ".lock" {
			os.Remove(conf.Dir0 + "/hosts/" + s)
		}
	}
}

func Config(dname string) {
	conf.Dir0 = dname
	//c := load_conf.LoadWaitMe(dname + "/conf/me.txt")

	var c node.Node
	load_conf.LoadWaitInterface(dname+"/conf/me.txt", &c)

	conf.I1 = c.I1
	conf.I2 = c.I2

	if c.I1 == "" || c.I2 == "" {
		log.Println("cannot crawl empty range")
		time.Sleep(time.Second)
		os.Exit(0)
	}

	conf.N = c.N

	nws, e := ioutil.ReadFile(dname + "/conf0/nworkers.txt")
	if e != nil {
		log.Fatal("cannot load nworkers file ", e)
	}
	if len(nws) <= 1 {
		log.Fatal("short nws", nws)
	}
	if nws[len(nws)-1] == '\n' {
		nws = nws[0 : len(nws)-1]
	}

	nw, e := strconv.Atoi(string(nws))
	if e != nil {
		log.Fatal("cannot parse nworkers ", e)
	}

	log.Println("nworkers=", nw)

	conf.NWorkers = nw

	nWorkers = nw

	conf.ProfilePort = strconv.Itoa(50000 + ra.Intn(100))

	ioutil.WriteFile(conf.Dir0+"/conf/pprof_port.txt", []byte(conf.ProfilePort), 0644)

	s, e := ioutil.ReadFile(conf.Dir0 + "/COU.txt")
	log.Println("initial COU", COU)
	if e == nil {
		c, e := strconv.Atoi(string(s))
		if e == nil {
			COU = int64(c)
		}
	}

	s, e = ioutil.ReadFile(conf.Dir0 + "/TIME.txt")
	log.Println("initial TIME", s)
	if e == nil {
		c, e := strconv.ParseInt(string(s), 10, 64)
		if e == nil {
			t0 = int64(c)
		}
	} else {
		ioutil.WriteFile(conf.Dir0+"/TIME.txt", []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0644)
	}

	log.Println("Profiler Port", conf.ProfilePort)

	//logf, err := os.OpenFile(dname+"/logs/main", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	//utils.Check(err, "main Config")
	//	log.SetOutput(logf)
	log.Println("\n\n\n\n\n\nstart:::>", time.Now())
}

func main() {
	//defer profile.Start().Stop()
	defer profile.Start(profile.MemProfile).Stop()

	var dir0 string

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

	utils.MakeDir0(conf.Dir0 + "/tmp_links")
	utils.MakeDir0(conf.Dir0 + "/tmp_links1")

	utils.MakeDir0(conf.Dir0 + "/tmp_crawler_links")
	utils.MakeDir0(conf.Dir0 + "/tmp_crawler_links1")

	DeleteLocks()

	pp := ra.Intn(60000)
	pps := strconv.Itoa(pp)
	go func() {
		log.Println(http.ListenAndServe("localhost:"+pps, nil))
	}()

	QueueWrite, _ = os.OpenFile(conf.Dir0+"/queue.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	QueueRead, _ = os.OpenFile(conf.Dir0+"/queue.txt", os.O_CREATE|os.O_RDONLY, 0644)
	QueuePoi, _ = os.OpenFile(conf.Dir0+"/queue.txt.poi", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)

	Crawled, _ = os.OpenFile(conf.Dir0+"/crawled.txt", os.O_CREATE|os.O_WRONLY, 0644)
	Submitted, _ = os.OpenFile(conf.Dir0+"/submitted.txt", os.O_CREATE|os.O_WRONLY, 0644)

	Records, _ = os.OpenFile(conf.Dir0+"/records.txt", os.O_CREATE|os.O_WRONLY, 0644)

	//	seed.Seed(jobs, conf.Dir0, conf.I1, conf.I2)

	fi, e := QueueWrite.Stat()
	if e != nil && fi != nil {
		R2 = int(fi.Size())
	}

	if fi.Size() < 10 {
		seed.Seed(jobs, conf.Dir0, conf.I1, conf.I2)
	}

	for w := 0; w <= nWorkers; w++ {
		go worker(w)
	}

	go AddJobs(0)

	for i := 0; i < 1; i++ {
		go OutgoingMessages()
	}

	for i := 0; i < 15; i++ {
		go ProcessResults()
	}

	Recorder()

}
