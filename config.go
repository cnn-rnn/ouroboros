package main

import (
	"bufio"
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	mrand "math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	node "ouroboros/src/node"

	cluster "ouroboros/src/cluster"
	utils "ouroboros/src/utils"
)

var nodeInfo = make(map[string]node.Node)

var me node.Node

var dir0 string

var seedIP = "174.138.39.149:7004"
var routingIP = "174.138.39.149:7003"
var coverIP = "174.138.39.149:7002"
var caIP = "174.138.39.149:7001"
var roleIP = "174.138.39.149:7005"
var bcclIP = "174.138.39.149:8002"

var rs = mrand.NewSource(time.Now().UnixNano())
var ra = mrand.New(rs)

type Conf struct {
	Dir0  string
	PrivK *rsa.PrivateKey
}

var conf Conf

var hash0 = ""

var x cluster.Cluster

var XCompressed = make([][]int, 0)
var XPower = make([][][]int, 0)

func GenerateCredentials(n int) {
	ph := strconv.Itoa(n)
	pp := strconv.Itoa(n + 1)
	ps := strconv.Itoa(n + 2)
	pr := strconv.Itoa(n + 3)
	pq := strconv.Itoa(n + 4)
	pg := strconv.Itoa(n + 5)

	PrivK, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}

	PubK := PrivK.Public()

	PrivKPEM := new(bytes.Buffer)
	pem.Encode(PrivKPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(PrivK),
	})
	ioutil.WriteFile(conf.Dir0+"conf/PrivKPEM.pem", PrivKPEM.Bytes(), 0644)

	PubKPEM := new(bytes.Buffer)
	p, e := x509.MarshalPKIXPublicKey(PubK)

	log.Println("pe,", string(p), e)

	pem.Encode(PubKPEM, &pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: p,
	})

	b := PubKPEM.Bytes()

	s1, e := ioutil.ReadFile(conf.Dir0 + "/conf0/nworkers.txt")
	if e != nil || len(s1) <= 1 {
		log.Fatal("cannot load nworkers", e)
	}
	if s1[len(s1)-1] == '\n' {
		s1 = s1[0 : len(s1)-1]
	}

	/*
		nw, e := strconv.Atoi(string(s1))
		if e != nil {
			log.Fatal("cannot convert s1", e)
		}
	*/

	s2, e := ioutil.ReadFile(conf.Dir0 + "/conf0/disk.txt")
	if e != nil || len(s2) <= 1 {
		log.Fatal("cannot load nworkers", e)
	}
	if s2[len(s2)-1] == '\n' {
		s2 = s2[0 : len(s2)-1]
	}

	disk, e := strconv.ParseFloat(string(s2), 64)
	if e != nil {
		log.Fatal("cannot convert s2", e)
	}

	me = node.Node{
		//		Id:     Id,
		PubK: string(b),
		Name: "Zalupa",

		//NWorkers: nw,

		Ip:     "",
		Port_h: ph,
		Port_p: pp,
		Port_s: ps,
		Port_r: pr,
		Port_q: pq,
		Port_g: pg,
		I1:     "",
		I2:     "",
		Pem:    "dummy",
		N:      -1,
		D:      disk,
	} // in GB

	s, _ := json.Marshal(me)
	ioutil.WriteFile(dir0+"/conf/me0.txt", []byte(s), 0644)
	log.Println("credential generated", me)
}

func LoadCredential(n int) {
	s, e := ioutil.ReadFile(dir0 + "/conf/me.txt")
	if e != nil {
		GenerateCredentials(n)
	} else {
		e = json.Unmarshal(s, &me)
		if e != nil {
			GenerateCredentials(n)
		} else {
			log.Println("credentials loaded, me = ", me)
		}
	}
}

func Sign(s []byte) string {
	h := sha1.New()
	h.Write([]byte(s))
	sum := h.Sum(nil)
	sig, _ := rsa.SignPKCS1v15(rand.Reader, conf.PrivK, crypto.SHA1, sum)
	return hex.EncodeToString(sig)
}

func Register() {
BEG:
	conn, ec := net.Dial("tcp", caIP)
	if ec != nil {
		log.Println("conf register : wating for caIP")
		time.Sleep(time.Second)
		goto BEG
	}
	s, _ := json.Marshal(me)
	n := len(s)
	n += 7
	ns := strconv.Itoa(n)
	if len(ns) > 7 {
		log.Println("ns", n, ns)
		os.Exit(0)
	}
	b := make([]byte, 7)
	for i := 0; i < 7 && i < len(ns); i++ {
		b[i] = ns[i]
	}
	conn.Write(append(b, append(s, 0x11)...))
	r := bufio.NewReader(conn)
	res, e := r.ReadBytes(0x11)
	if e != nil || string(res[0:len(res)-1]) != "success" {
		log.Println("did not register", string(s), e)
		conn.Close()
		return
	}
	me1, e := r.ReadBytes(0x11)

	if len(me1) < 1 {
		log.Println("short string received")
		time.Sleep(time.Second)
		os.Exit(0)
	}
	me1 = me1[:len(me1)-1]

	var me3 node.Node
	e = json.Unmarshal(me1, &me3)
	if e != nil {
		log.Println("cannot unmarshal my data, as received from CA", string(me1), me1)
		os.Exit(0)
	}
	me = me3

	ioutil.WriteFile(conf.Dir0+"/conf/me1.txt", me1, 0644)
	conn.Close()
}

func GetWaitSeed() {
BEG:
	conn, ec := net.Dial("tcp", seedIP)
	if ec != nil {
		log.Println("conf : wating for seedIP")
		time.Sleep(time.Second)
		goto BEG
	}
	defer conn.Close()
	if me.Id == "" {
		log.Println("my id is empty!")
		time.Sleep(45 * time.Second)
		goto BEG
	}

	conn.Write(append([]byte(me.Id), '\n'))
	conn.Write([]byte("seed\n"))
	r := bufio.NewReader(conn)
	s, _ := r.ReadString(0x11)
	ioutil.WriteFile(dir0+"/seed/seed.txt", []byte(s), 0644)
}

func GetClusterData() {
BEG:
	conn, ec := net.Dial("tcp", bcclIP)
	if ec != nil {
		log.Println("conf : wating for bcclIP")
		time.Sleep(time.Second)
		goto BEG
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	var s1 []byte
	var s []byte
	var e error
	for e != io.EOF {
		s1, e = r.ReadBytes(0x11)
		if e != nil && e != io.EOF {
			conn.Close()
			goto BEG
		}
		//log.Println("s1", len(s1)) //, string(s1))
		if e == io.EOF {
			break
		}
		s = s1
	}

	if len(s) <= 1 {
		log.Println("short s=", string(s))
		time.Sleep(time.Second)
		conn.Close()
		goto BEG
	}
	s = s[0 : len(s)-1]
	if string(s) == "wait" {
		log.Println("waiting")
		time.Sleep(10 * time.Second)
		goto BEG
	}
	h := sha1.New()
	h.Write([]byte(s))

	hash1 := string(h.Sum(nil))
	if hash1 == hash0 {
		log.Println("cluster configuration the same")
	} else {
		log.Println("Reconfiguring")
		hash0 = hash1
		ioutil.WriteFile(conf.Dir0+"/net/cluster.txt", []byte(s), 0644)

		t1 := time.Now().UnixNano()

		ioutil.WriteFile(conf.Dir0+"/net/cluster_"+strconv.FormatInt(t1, 10)+".txt", []byte(s), 0644)

		Reconfigure(s)
	}
}

func Reconfigure(s []byte) {
	e := json.Unmarshal(s, &x)
	if e != nil {
		log.Println("cannot reconfigure", e)
		return
	}

	x.Rou.SetPointer(&x.Nodes)
	x.RT.SetPointer(&x.Nodes)

	log.Println("reconfigure x=", x.Strong())

	sh := sha256.Sum256([]byte(me.PubK))
	Id := fmt.Sprintf("%x", sh)
	me.Id = Id
	me2 := x.Nodes[me.Id]
	me.I1 = me2.I12
	me.I2 = me2.I22
	s1, _ := json.Marshal(me)
	ioutil.WriteFile(dir0+"/conf/me.txt", []byte(s1), 0644)
	s0, _ := json.Marshal(x.Nodes)
	ioutil.WriteFile(dir0+"/net/nodes.txt", []byte(s0), 0644)

	//---------cover

	cov := x.Cov
	s2, _ := json.Marshal(cov)
	ioutil.WriteFile(dir0+"/net/cover.txt", []byte(s2), 0644)

	//---------routing

	routes := x.CreateRoutes(me.Id)

	log.Println("routes:", routes)

	s3, e := json.Marshal(routes)
	e = ioutil.WriteFile(dir0+"/net/routing.txt", []byte(s3), 0644)

	//---------rtree

	V := x.ImmediateContacts(me.Id)

	log.Println("immediate contacts:", V)

	s4, e := json.Marshal(V)
	ioutil.WriteFile(dir0+"/net/contacts.txt", []byte(s4), 0644)

	myrole := x.RT.RoleMap[me.Id]
	s5, _ := json.Marshal(myrole)
	ioutil.WriteFile(dir0+"/net/role.txt", []byte(s5), 0644)

}

func Update() {
	for {
		GetClusterData()
		time.Sleep(30 * time.Second)
	}
}

func Config(dir0 string) {
	utils.MakeDir0(dir0)
	utils.MakeDir0(dir0 + "/logs")

	seedIP = os.Getenv("seedIP")
	routingIP = os.Getenv("routingIP")
	coverIP = os.Getenv("coverIP")
	caIP = os.Getenv("caIP")
	roleIP = os.Getenv("roleIP")
	bcclIP = os.Getenv("bcclIP")
}

func main() {

	//dir0 = os.Args[1]
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

	log.Println("configuring", dir0)

	conf.Dir0 = dir0
	Config(dir0)

	utils.MakeDir0(dir0)
	utils.MakeDir0(dir0 + "/conf")
	utils.MakeDir0(dir0 + "/hosts")
	utils.MakeDir0(dir0 + "/resou")
	utils.MakeDir0(dir0 + "/inbox_crawler")
	utils.MakeDir0(dir0 + "/inbox_ranker")
	utils.MakeDir0(dir0 + "/outbox")
	utils.MakeDir0(dir0 + "/outbox1")
	utils.MakeDir0(dir0 + "/net")
	utils.MakeDir0(dir0 + "/rank_pointers")
	utils.MakeDir0(dir0 + "/seed")
	utils.MakeDir0(dir0 + "/tmp")
	utils.MakeDir0(dir0 + "/hist")
	utils.MakeDir0(dir0 + "/p")
	utils.MakeDir0(dir0 + "/x")
	utils.MakeDir0(dir0 + "/tx")
	utils.MakeDir0(dir0 + "/ranks")
	utils.MakeDir0(dir0 + "/gateway")
	utils.MakeDir0(dir0 + "/rtree")
	utils.MakeDir0(dir0 + "/index")
	utils.MakeDir0(dir0 + "/pages")
	utils.MakeDir0(dir0 + "/tmp_ranker")
	utils.MakeDir0(dir0 + "/tmp_index")
	utils.MakeDir0(dir0 + "/tmp_index1")
	utils.MakeDir0(dir0 + "/tmp_crawler")
	utils.MakeDir0(dir0 + "/inlinks")
	utils.MakeDir0(dir0 + "/outlinks")

	var n int
	if len(os.Args) > 2 {
		var e error
		n, e = strconv.Atoi(os.Args[2])
		if e != nil {
			log.Println("port must be int")
			os.Exit(0)
		}
	} else {
		n = 60600 + ra.Intn(100)
	}
	LoadCredential(n)

	if _, err := os.Stat(conf.Dir0 + "/conf/me1.txt"); err != nil {
		log.Println("registering")
		Register()
		log.Println("regis done")
	} else {
		log.Println("already registered")
	}

	f, e := os.OpenFile(conf.Dir0+"/seed/seed.txt", os.O_RDONLY, 0644)
	B := false
	if e != nil {
		B = true
	} else {
		fi, _ := f.Stat()
		if fi.Size() < 1000 {
			B = true
		}
		f.Close()
	}

	if B {
		log.Println("reloading seed")
		GetWaitSeed()
		log.Println("seed done")
	}

	Update()
}
