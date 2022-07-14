package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	btma_prefix "ouroboros/src/btma_prefix"
	"ouroboros/src/btms"
	"ouroboros/src/load_conf"
	node "ouroboros/src/node"

	cluster "ouroboros/src/cluster"
)

type Conf struct {
	Dir0 string
	Me   node.Node
}

var conf Conf

type hostHandler struct {
	Z string
}

func (x *hostHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	host := req.URL.Path
	if len(host) <= 6 {
		fmt.Fprintf(w, "wrong host name")
		return
	}
	host = host[6:]

	log.Println("host --->", host)

	h := btms.OpenIfExists(nil, conf.Dir0+"/hosts", host)
	if h == nil {
		fmt.Fprintf(w, "nothing")
		return
	}

	q := h.AsList(0)
	for i := range q {
		fmt.Fprintf(w, q[i]+"\n")
	}
}

func hello(w http.ResponseWriter, req *http.Request) {

	log.Println("say hello")

	fmt.Fprintf(w, "hello\n")
}

func headers(w http.ResponseWriter, req *http.Request) {

	for name, headers := range req.Header {
		for _, h := range headers {
			fmt.Fprintf(w, "%v: %v\n", name, h)
		}
	}
}

func host_list(w http.ResponseWriter, req *http.Request) {

	log.Println("accessed host_list")

	h := btma_prefix.OpenIfExists(nil, conf.Dir0+"/hostsd", "host.txt")
	q := h.AsList(0)

	sort.Strings(q)

	/*/
	for i := range q {
		log.Println("|||" + q[i] + "|||")
	}
	*/

	log.Println("host_list reached,len(hosts)", len(q))

	for i := range q {
		fmt.Fprintf(w, q[i]+"\n")
	}
}

func host_urls(host string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := btms.OpenOrCreate(nil, conf.Dir0+"/hosts", host)
		q := h.AsList(0)
		for i := range q {
			fmt.Fprintf(w, q[i]+"\n")
		}
	})
}

func cou(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/COU.txt")

	log.Println("coucou", s, e, string(s))

	if e != nil {
		log.Println("COU error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func general(w http.ResponseWriter, req *http.Request, fname string) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/" + fname)
	if e != nil {
		log.Println("outbox Cou error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func ostat(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/outbox_data/ostat.txt")

	log.Println("ostat", s)

	if e != nil {
		log.Println("log gateway error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func lstat(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/links_data/lstat.txt")
	if e != nil {
		log.Println("log gateway error ", e)
	}

	var Cou map[string]int
	e = json.Unmarshal(s, &Cou)
	if e != nil {
		fmt.Fprintf(w, e.Error())
		return
	}
	p := "<html><body><p>Links statistics:</p><tablae>"
	for id := range Cou {
		p += "<tr>"
		p += "<th>" + id + "</th>"
		p += "<th>" + strconv.Itoa(Cou[id]) + "</th>"
		p += "</tr>"
	}
	p += "</table></body></html>"

	fmt.Fprintf(w, string(p))
}

func istat(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/links_data/istat.txt")
	if e != nil {
		log.Println("log gateway error ", e)
	}

	var Cou map[string]int
	e = json.Unmarshal(s, &Cou)
	if e != nil {
		fmt.Fprintf(w, e.Error())
		return
	}
	p := "<html><body><p>Inbox statistics:</p><tablae>"
	for id := range Cou {
		p += "<tr>"
		p += "<th>" + id + "</th>"
		p += "<th>" + strconv.Itoa(Cou[id]) + "</th>"
		p += "</tr>"
	}
	p += "</table></body></html>"

	fmt.Fprintf(w, string(p))
}

func outbox_cou(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/outbox_data/Cou_0.txt")
	if e != nil {
		log.Println("outbox Cou error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func cluster_info(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/net/cluster.txt")

	var c cluster.Cluster

	e = json.Unmarshal(s, &c)

	c.Rou.SetPointer(&c.Nodes)
	c.RT.SetPointer(&c.Nodes)

	if e != nil {
		log.Println("cluster ", e)
	}

	fmt.Fprintf(w, c.Strong())

	s, e = ioutil.ReadFile(conf.Dir0 + "/outbox_data/Cou_0")
	var a map[string]int
	e = json.Unmarshal(s, &a)

	log.Println("e=e", e)

	u := ""
	for i := range a {
		u += i + ":" + strconv.Itoa(a[i]) + "\n"
	}
	u += "\n"

	log.Println(u)

	fmt.Fprintf(w, u)
}

func istat_data(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/inbox_data/idata.txt")
	if e != nil {
		log.Println("log rtree error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func ostat_data(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/outbox_data/odata.txt")
	if e != nil {
		log.Println("log rtree error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func rtree(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/rtree.log")
	if e != nil {
		log.Println("log rtree error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func gateway(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/gateway.log")
	if e != nil {
		log.Println("log gateway error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func qserver(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/qserver.log")
	if e != nil {
		log.Println("log qserver error ", e)
	}
	fmt.Fprintf(w, string(s))
}
func xator(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/xator.log")
	if e != nil {
		log.Println("log qserver error ", e)
	}
	fmt.Fprintf(w, string(s))
}
func organizer(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/organizer.log")
	if e != nil {
		log.Println("log qserver error ", e)
	}
	fmt.Fprintf(w, string(s))
}
func crawler(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile("/var/crawler.log")
	if e != nil {
		log.Println("log qserver error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_main(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/main")
	if e != nil {
		log.Println("log main error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_main_status(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/main.status")
	if e != nil {
		log.Println("log main status error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_inbox(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/inbox")
	if e != nil {
		log.Println("log inbox error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_inbox_status(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/inbox.status")
	if e != nil {
		log.Println("log inbox status error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_outbox(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/outbox")
	if e != nil {
		log.Println("log outbox error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func log_outbox_status(w http.ResponseWriter, req *http.Request) {
	s, e := ioutil.ReadFile(conf.Dir0 + "/logs/outbox.status")
	if e != nil {
		log.Println("log outbox status error ", e)
	}
	fmt.Fprintf(w, string(s))
}

func statistics(w http.ResponseWriter, req *http.Request) {
	y, e := ioutil.ReadFile(conf.Dir0 + "/COU.txt")
	if e != nil {
		log.Println("log outbox status error ", e)
	}

	s := "<html><body>"
	s = "<p> Total number of pages crawled: " + string(y) + "</br></br>"

	y, e = ioutil.ReadFile(conf.Dir0 + "/inbox_data/istat.txt")
	if e != nil {
		log.Println("log outbox status error ", e)
	}
	var u map[string]int
	json.Unmarshal(y, &u)
	s += "Incomings Msgs: </br>"
	for id := range u {
		s += id + "->" + strconv.Itoa(u[id]) + "</br>"
	}
	s += "</br>"

	y, e = ioutil.ReadFile(conf.Dir0 + "/outbox_data/lstat.txt")
	if e != nil {
		log.Println("log outbox status error ", e)
	}
	json.Unmarshal(y, &u)
	s += "Outgoing Msgs: </br>"
	for id := range u {
		s += id + "->" + strconv.Itoa(u[id]) + "</br>"
	}
	s += "</body></html>"

	fmt.Fprintf(w, string(s))
}

func GetMainPid() string { // pgrep "process name"
	r, e := ioutil.ReadFile(conf.Dir0 + "/logs/main.status")
	if e != nil {
		return ""
	}
	s := string(r)
	q := strings.Split(s, "\n")
	return q[0]
}

func ram(w http.ResponseWriter, req *http.Request) {

	pid := GetMainPid()
	if pid == "" {
		return
	}

	cmd := exec.Command("ps", "-p", pid, "-o", "%mem")

	// ps -p 11685 -o %mem
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Println(err)
	if err != nil {
		return
	}
	s := out.String()
	q := strings.Split(s, "\n")
	//	fmt.Println("result", len(q), "number=", q[1])
	fmt.Fprintf(w, q[1])

}

func GetInstanceId() string {

	cmd := exec.Command("wget", "-q", "-O", "-", "http://169.254.169.254/latest/meta-data/instance-id")

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Println(err)
	if err != nil {
		log.Println("GetInstanceId", err)
		return ""
	}
	s := out.String()
	return s
}

func Ip(w http.ResponseWriter, req *http.Request) {

	cmd := exec.Command("wget", "-qO-", "http://instance-data/latest/meta-data/public-ipv4")

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Println(err)
	if err != nil {
		log.Fatal(err)
	}
	s := out.String()
	fmt.Fprintf(w, s)
}

func front(w http.ResponseWriter, req *http.Request) {
	s := "<html><body>"
	s += "<a style =\"float:right;  margin-right: 20px; \" href= \"cluster  \" >cluster</a>"
	s += "<a style =\"float:right;  margin-right: 20px; \" href= \"host_list  \" >host_list</a>"
	s += "<a style =\"float:right;  margin-right: 20px; \" href= \"by_host_form  \" >by_host</a>"
	s += "<a style =\"float:right;  margin-right: 20px; \" href= \"statistics  \" >statistics</a>"

	s += "<p style = \" text-align: center;\">  NodeId </br> " + conf.Me.Id + "</p></br>"
	s += "</body></html>"

	fmt.Fprintf(w, s)

}

var form = `<html><title>Local host search </title> <body> 
<style> form {	text-align: center;  margin-top: 10em; }</style>  

<form  action="/by_host" method="GET" > Urls by host. Enter Host: <br> 
<input  name="query" value=""> 
<br>
<input type = "submit" > 
</form>   

</body> </html>`

func by_host_form(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, form)
}

func by_host(w http.ResponseWriter, r *http.Request) {
	Q0 := r.URL.Query()
	host := Q0["query"]

	h := btms.OpenIfExists(nil, conf.Dir0+"/hosts", host[0])
	if h == nil {
		fmt.Fprintf(w, "nothing")
		return
	}

	q := h.AsList(0)
	for i := range q {
		fmt.Fprintf(w, q[i]+"\n")
	}
}

func main() {
	if len(os.Args) > 1 {
		conf.Dir0 = os.Args[1]
	} else {
		conf.Dir0 = "/d"
	}

	load_conf.LoadWaitInterface(conf.Dir0+"/conf/me.txt", &conf.Me)

	http.HandleFunc("/ram", ram)
	http.HandleFunc("/ip", Ip)

	http.HandleFunc("/hello", hello)
	http.HandleFunc("/headers", headers)

	http.HandleFunc("/main", log_main)
	http.HandleFunc("/main_status", log_main_status)
	http.HandleFunc("/inbox", log_inbox)
	http.HandleFunc("/inbox_status", log_inbox_status)
	http.HandleFunc("/outbox", log_outbox)
	http.HandleFunc("/outbox_status", log_outbox_status)

	http.HandleFunc("/rtree", rtree)
	http.HandleFunc("/qserver", qserver)
	http.HandleFunc("/gateway", gateway)
	http.HandleFunc("/xator", xator)
	http.HandleFunc("/organizer", organizer)
	http.HandleFunc("/crawler", crawler)
	http.HandleFunc("/host_list", host_list)
	http.HandleFunc("/cou", cou)

	http.HandleFunc("/ostat", ostat)
	http.HandleFunc("/istat", istat)
	http.HandleFunc("/lstat", lstat)

	http.HandleFunc("/istat_data", istat_data)
	http.HandleFunc("/ostat_data", ostat_data)

	http.HandleFunc("/cluster", cluster_info)

	http.HandleFunc("/by_host_form", by_host_form)
	http.HandleFunc("/by_host", by_host)

	http.HandleFunc("/statistics", statistics)

	http.HandleFunc("/", front)

	mux := http.NewServeMux()

	hh := hostHandler{Z: "zzzzzzzzzz"}

	mux.Handle("/host/", &hh)

	go http.ListenAndServe(":30000", mux)

	http.ListenAndServe(":8090", nil)
}
