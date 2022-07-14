package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	cluster "ouroboros/src/cluster"
	"ouroboros/src/constants"
	node "ouroboros/src/node"
	"sort"
	"strconv"
	"sync"
	"time"
)

var caIPo = "174.138.39.149:7002"
var bcclIP = "174.138.39.149:8002"

type Conf struct {
	Dir0  string
	Dir1  string
	Nodes map[string]node.Node
}

var conf Conf

var clust cluster.Cluster

var hash0 string

var Stat = make(map[string]string)
var COU = make(map[string]string)

func GetList() {
	s, e := ioutil.ReadFile(conf.Dir0 + "/node_list.txt")
	if e != nil {
		return
	}
	var x map[string]node.Node
	e = json.Unmarshal(s, &x)
	conf.Nodes = x
}

func UpdateNodeList() {
	for {
		GetList()
		Poll()
		time.Sleep(60 * time.Second)
	}
}

func if_alive() {
	for {
		Alive()
		time.Sleep(60 * time.Second)
	}
}

func Alive() {
	for i := range clust.Nodes {
		go alive(clust.Nodes[i])
	}
}

func alive(p node.Node) {

	log.Println("alive cou request", p.Id)

	w, e := Get(p, "cou")
	s := w
	if e != nil {
		s += e.Error()
	}
	COU[p.Id] = s
	fmt.Println("alive", p.Id, s)
}

func Pull(q string) map[string]string {
	x := make(map[string]string)
	for i := range clust.Nodes {
		s, e := Get(clust.Nodes[i], q)
		if e == nil {
			x[i] = s
		} else {
			x[i] = e.Error()
		}
	}
	return x
}

func Packages(w http.ResponseWriter, req *http.Request) {

	in := Pull("istat")
	out := Pull("ostat")

	log.Println("in=", in)
	log.Println("out=", out)

	var u map[string]int

	keys := make([]string, 0)
	for id := range clust.Nodes {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	s := "<html><body>In</br>"
	for i := range keys {
		id := keys[i]
		s += id + ":</br>"
		json.Unmarshal([]byte(in[id]), &u)
		for j := range u {
			s += j + "->" + strconv.Itoa(u[j]) + "</br>"
		}
		s += "<br></br>"

	}
	s += "</br>Out</br>"
	for i := range keys {
		id := keys[i]
		s += id + ":</br>"

		json.Unmarshal([]byte(out[id]), &u)
		for j := range u {
			s += j + "->" + strconv.Itoa(u[j]) + "</br>"
		}
		s += "</br><br>"

	}
	s += "</body></html>"

	fmt.Fprintf(w, (s))

}

func Poll() {
	for i := range clust.Nodes {
		go poll(clust.Nodes[i])
	}
}

func get_stat(wg *sync.WaitGroup, mx *sync.Mutex, what string, cou *map[string]map[string]int, id string) {
	s, e := Get(clust.Nodes[id], what)
	if e == nil {
		var x map[string]int
		e = json.Unmarshal([]byte(s), &x)
		if e == nil {
			mx.Lock()
			(*cou)[id] = x
			mx.Unlock()
		}
	}
	wg.Done()
}

func Inbox_Stat(w http.ResponseWriter, req *http.Request) {
	var wg_i sync.WaitGroup
	var mx_i sync.Mutex
	icou := make(map[string]map[string]int)
	for id := range clust.Nodes {
		wg_i.Add(1)
		go get_stat(&wg_i, &mx_i, "istat", &icou, id)
	}

	var wg_o sync.WaitGroup
	var mx_o sync.Mutex
	ocou := make(map[string]map[string]int)
	for id := range clust.Nodes {
		wg_o.Add(1)
		go get_stat(&wg_o, &mx_o, "ostat", &ocou, id)
	}

	wg_i.Wait()
	wg_o.Wait()

	keys := make([]string, 0)
	for id := range clust.Nodes {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	s := "<html><body><p>Incoming msg statistics</p><table><tr>"
	for i := range keys {
		s += "<th>" + keys[i][0:4] + "</th>"
	}
	s += "</tr>"
	for i := range keys {
		id1 := keys[i]
		s += "<tr><th>" + id1[0:4] + "</th>"
		for j := range keys {
			id2 := keys[j]
			s += "<th>" + strconv.Itoa(icou[id1][id2]) + "</th>"
		}
		s += "</tr>"
	}

	s += "</table>"

	s = "<p>Incoming msg statistics</p><table><tr>"
	for i := range keys {
		s += "<th>" + keys[i][0:4] + "</th>"
	}
	s += "</tr>"
	for i := range keys {
		id1 := keys[i]
		s += "<tr><th>" + id1[0:4] + "</th>"
		for j := range keys {
			id2 := keys[j]
			s += "<th>" + strconv.Itoa(ocou[id1][id2]) + "</th>"
		}
		s += "</tr>"
	}
	s += "</table>"

	s = "<p>Differences</p><table><tr>"
	for i := range keys {
		s += "<th>" + keys[i][0:4] + "</th>"
	}
	s += "</tr>"
	for i := range keys {
		id1 := keys[i]
		s += "<tr><th>" + id1[0:4] + "</th>"
		for j := range keys {
			id2 := keys[j]
			s += "<th>" + strconv.Itoa(ocou[id1][id2]-icou[id1][id1]) + "</th>"
		}
		s += "</tr>"
	}
	s += "</table></body></html>"

	fmt.Fprintf(w, string(s))
}

func Get(p node.Node, code string) (string, error) {
	cmd := exec.Command("curl", "--max-time", "10", p.Ip+":8090/"+code)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Println("Get", err)
	if err != nil {
		return "", err
	}
	s := out.String()
	return s, nil
}

func poll(p node.Node) {
	ram, e1 := Get(p, "ram")
	main_s, e2 := Get(p, "main_status")
	inbox, e3 := Get(p, "inbox_status")
	outbox, e4 := Get(p, "outbox_status")
	s := ""
	s += ram
	if e1 != nil {
		s += e1.Error()
	}
	s += main_s
	if e2 != nil {
		s += e2.Error()
	}
	s += inbox
	if e3 != nil {
		s += e3.Error()
	}
	s += outbox
	if e4 != nil {
		s += e4.Error()
	}
	Stat[p.Id] = s
	fmt.Println(p.Id, s)
}

func getClusterFromBC() (*cluster.Cluster, bool, error) {
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
		log.Println("s= wait, we do not need to wait")
		conn.Close()
		return nil, false, nil
		time.Sleep(10 * time.Second)
		goto BEG
	}

	h := sha1.New()
	h.Write([]byte(s))
	hash1 := string(h.Sum(nil))
	if hash1 == hash0 {
		log.Println("cluster configuration the same")
		conn.Close()
		return nil, true, nil
	} else {
		hash0 = hash1

		ioutil.WriteFile(conf.Dir0+"/net/cluster_hash.txt", []byte(hash0), 0644)
		ioutil.WriteFile(conf.Dir0+"/net/cluster.txt", []byte(s), 0644)

		var cl cluster.Cluster
		e := json.Unmarshal(s, &cl)
		conn.Close()
		return &cl, false, e
	}
}

func Cluster_info(w http.ResponseWriter, req *http.Request) {

	cou := Pull("cou")

	keys := make([]string, 0)
	for id := range clust.Nodes {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	s := "<html><body>Cluster<br>"
	for i := range keys {
		id := keys[i]
		s += "id:" + id + "</br>status:" + clust.Nodes[id].Status + "</br>Ip</br>"
		if clust.Nodes[id].Status == constants.Active {
			s += "<a href=\" http://" + clust.Nodes[id].Ip + ":8090\" >" + clust.Nodes[id].Ip + "</a>:"
		} else {
			s += clust.Nodes[id].Ip + ":"
		}
		s += "</br>range:" + clust.Nodes[id].I12 + "-" + clust.Nodes[id].I22 + ":</br># of urls crawled  " + cou[id] + "</br></br>"
	}
	s += "</body></html>"
	fmt.Fprintf(w, (s))
}

func Cluster_info_file() string {

	cou := Pull("cou")

	keys := make([]string, 0)
	for id := range clust.Nodes {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	s := "<html><body>Cluster<br>"
	for i := range keys {
		id := keys[i]
		s += "id:" + id + "</br>status:" + clust.Nodes[id].Status + "</br>Ip</br>"
		if clust.Nodes[id].Status == constants.Active {
			s += "<a href=\" http://" + clust.Nodes[id].Ip + ":8090\" >" + clust.Nodes[id].Ip + "</a>:"
		} else {
			s += clust.Nodes[id].Ip + ":"
		}
		s += "</br>range:" + clust.Nodes[id].I12 + "-" + clust.Nodes[id].I22 + ":</br># of urls crawled  " + cou[id] + "</br></br>"
	}
	s += "</body></html>"
	return s
}

func GetKeys(x map[string]string) []string {
	keys := make([]string, len(x))
	i := 0
	for k := range x {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func Display(w http.ResponseWriter, req *http.Request) {
	keys := GetKeys(Stat)
	s := "<html><body>"
	for j := 0; j < len(keys); j++ {
		//		u, _ := json.Marshal(Stat[i])
		i := keys[j]
		u := Stat[i]
		s += "<p>" + i + "   " + conf.Nodes[i].Ip + "   " + string(u) + "\n" + "</p>"
	}
	s += "</body></html>"
	fmt.Fprintf(w, string(s))
}

func ClusterSimple(w http.ResponseWriter, req *http.Request) {
	keys := GetKeys(Stat)
	s := "<html><body>"
	for j := 0; j < len(keys); j++ {
		//		u, _ := json.Marshal(Stat[i])
		i := keys[j]
		u := COU[i]
		s += "<p>" + i + "   " + conf.Nodes[i].Ip + "  " + conf.Nodes[i].I1 + "   " + conf.Nodes[i].I2 + "   " + string(u) + "\n" + "</p>"
	}
	s += "</body></html>"
	fmt.Fprintf(w, string(s))
}

func ClusterConf(w http.ResponseWriter, req *http.Request) {
	s := "<html><body>"
	//s += clust.String1()

	keys := make([]string, 0)
	for i := range clust.Nodes {
		keys = append(keys, i)
	}
	sort.Strings(keys)

	for j := range keys {
		i := keys[j]
		s += "<a href = \"http://" + clust.Nodes[i].Ip + "\">" + clust.Nodes[i].Id + "</a>  " + clust.Nodes[i].I12 + ":" + clust.Nodes[i].I22 + "</br>"
	}

	s += "</br></br>"

	fmt.Println("COU", COU)

	s += "COU data </br>"

	for j := range keys {
		i := keys[j]
		s += "<a href = \"http://" + clust.Nodes[i].Ip + ":8080\">" + i + "</a>:" + COU[i] + "</br>"
	}

	s += "</br></br>"

	for l := range clust.Cov.A {
		s += "l=" + strconv.Itoa(l) + "</br>"
		for n := 0; n < len(clust.Cov.A[l]); n++ {
			id := clust.Cov.A[l][n]
			s += clust.Cov.A[l][n] + ">range:" + clust.Nodes[id].I12 + ":" + clust.Nodes[id].I22 + "</br>"
		}
		s += "</br>"
	}

	s += "role map </br>"
	for l := 0; l < len(clust.RT.LayerMap); l++ {
		s += "l=" + strconv.Itoa(l) + "</br>"
		for n := 0; n < len(clust.RT.LayerMap[l]); n++ {
			s += "n=" + strconv.Itoa(n) + "</br>"
			for i := 0; i < len(clust.RT.LayerMap[l][n]); i++ {
				s += "i=" + strconv.Itoa(i) + "</br>"
				s += clust.RT.LayerMap[l][n][i] + "</br>"
			}
			s += "</br>"
		}
		s += "</br>"
	}

	s += "</body></html>"
	fmt.Fprintf(w, string(s))
}

func pull_conf() {
	for {
		cl1, t, _ := getClusterFromBC()
		if !t {
			clust = *cl1
		}

		log.Println("pulled conf from BC")

		s := Cluster_info_file()

		ioutil.WriteFile("/var/www/rorur.com/cluster_info.txt", []byte(s), 0644)

		time.Sleep(100 * time.Second)
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

	var dir1 string
	if len(os.Args) > 2 {
		dir1 = os.Args[2]
	} else {
		dir1 = "/d"
	}
	conf.Dir1 = dir1

	//go UpdateNodeList()

	go pull_conf()

	//go if_alive()

	http.HandleFunc("/dashboard", Display)
	http.HandleFunc("/cluster_basic", ClusterSimple)
	http.HandleFunc("/cluster_conf", ClusterConf)
	http.HandleFunc("/cluster_info", Cluster_info)
	http.HandleFunc("/traffic", Packages)

	http.ListenAndServe(":3000", nil)

}
