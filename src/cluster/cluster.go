package cluster

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	mrand "math/rand"
	"net"
	"os/exec"
	"ouroboros/src/constants"
	cover "ouroboros/src/cover"
	"ouroboros/src/matri"
	node "ouroboros/src/node"
	"ouroboros/src/return_tree"
	"ouroboros/src/routing"
	"sort"
	"strconv"
	"time"
)

type Cluster struct {
	D       int
	L       int
	Dobro   int
	Streams int

	Nodes map[string]node.Node
	Cov   cover.Cover

	Rou routing.Routing

	RT return_tree.ReturnTree

	LastHashes map[string]string

	BCHasChanged     bool
	StatusHasChanged bool

	Hash string
}

func Init(D int, L int, Dobro int, Streams int) Cluster {
	return Cluster{D: D, L: L, Dobro: Dobro, Streams: Streams}
}

func (x *Cluster) Submit(bcip string) {

BEG:
	conn, ec := net.Dial("tcp", bcip)
	if ec != nil {
		log.Println("submit : wating for peerIP")
		time.Sleep(time.Second)
		goto BEG
	}
	s, _ := json.Marshal(*x)
	n := len(s)
	n += 7
	ns := strconv.Itoa(n)
	if len(ns) > 7 {
		log.Fatal("ns", n, ns)
	}
	b := make([]byte, 7)
	for i := 0; i < 7 && i < len(ns); i++ {
		b[i] = ns[i]
	}
	res, e := conn.Write(append(b, append(s, 0x11)...))
	log.Println("cluster now ", x.Strong())
	log.Println("submit", res, e)
	conn.Close()
}

func getNodesFromCA(caip string) (map[string]*node.Node, map[string]string, error) {
BEG:
	conn, ec := net.Dial("tcp", caip)
	if ec != nil {
		log.Println("cluster : wating for CAIPo")
		time.Sleep(time.Second)
		goto BEG
	}
	defer conn.Close()

	no1 := make(map[string]*node.Node)
	lh1 := make(map[string]string)

	r := bufio.NewReader(conn)
	for {
		s, e := r.ReadBytes(0x11)

		if e != nil {
			log.Println("gettin Nodes e", e)
			if e == io.EOF {
				break
			} else {
				conn.Close()
				goto BEG
			}
		}

		var node node.Node
		e = json.Unmarshal(s[0:len(s)-1], &node)
		if e != nil {
			log.Println("cannot unmarshal", e)
			conn.Write([]byte("cannot unmarshal\n"))
			continue
		}
		Id := node.Id
		no1[Id] = &node

		h := sha256.New()
		h.Write([]byte(s[:len(s)-1]))
		ha := fmt.Sprintf("%x", h.Sum(nil))
		lh1[Id] = ha
	}
	return no1, lh1, nil
}

func (x *Cluster) Add(no1 map[string]*node.Node) { // change routing matrix every time
	if len(x.Cov.A) == 0 {
		x.Cov.A = make([][]string, 1)
		x.Cov.A[0] = make([]string, 0)
	}
	keys0 := make([]string, 0)
	ex := make(map[string]bool)
	for id := range x.Nodes {
		keys0 = append(keys0, id)
		ex[id] = true
	}
	keys1 := make([]string, 0)
	for id := range no1 {
		keys1 = append(keys1, id)
	}
	if len(x.Nodes) == 0 {
		x.Nodes = make(map[string]node.Node)
	}
	x.Cov.SetPointer(&x.Nodes)
	x.RT.SetPointer(&x.Nodes)
	x.Rou.SetPointer(&x.Nodes)
	cou := 0
	for id := range no1 {
		if !ex[id] {
			x.Nodes[id] = *(no1[id])
			node := *(no1[id])
			I12, I22, _, _, _ := x.Cov.CalculateDocRangeToNode(node)
			node.I12 = I12
			node.I22 = I22
			x.Nodes[id] = node
			cou += 1
		}
	}
	var e error
	x.Rou, e = routing.CreateRoutingMatrix(&(x.Nodes), x.Streams)
	if e != nil {
		log.Println(e)
	}
	x.RT, e = return_tree.CreateRTree(&(x.Nodes), x.L, x.D, x.Dobro)
	if e != nil {
		log.Println(e)
	}

}

func (x *Cluster) Regenerate() {
	// do not touch the cover, just change RT & Routing

	log.Println("regenerate )(")

	var e error

	x.Rou.SetPointer(&x.Nodes)
	x.Rou, e = routing.CreateRoutingMatrix(&(x.Nodes), x.Streams)
	if e != nil {
		log.Println(e)
	}

	log.Println("Rou", x.Rou.Strong(), x.Rou.RM, x.Nodes)

	x.RT.SetPointer(&x.Nodes)
	x.RT, e = return_tree.CreateRTree(&(x.Nodes), x.L, x.D, x.Dobro)

	if e != nil {
		log.Println(e)
	}

}

func (x *Cluster) Persist(dir0 string) {
	s, _ := json.Marshal(*x)
	ioutil.WriteFile(dir0+"/cluster.txt", s, 0644)
	h := sha1.New()
	h.Write([]byte(s))
	x.Hash = string(h.Sum(nil))
	ioutil.WriteFile(dir0+"/latest_cluster_hash.txt", []byte(x.Hash), 0644)
	ioutil.WriteFile(dir0+"/"+base64.URLEncoding.EncodeToString([]byte(x.Hash))+".txt", s, 0644)
	log.Println("cluster=\n", x.Strong())
}

func (x *Cluster) Strong() string {
	s := ""
	sl := make([]string, 0)
	for id := range x.Nodes {
		sl = append(sl, id)
	}
	sort.Strings(sl)
	for i := range sl {
		id := sl[i]
		s += x.Nodes[id].String() + "\n"
	}
	s += x.Cov.Strong() + "\n"
	s += x.Rou.Strong() + "\n"
	s += x.RT.Strong() + "\n"
	return s
}

func Get(p node.Node, code string) (string, error) {
	cmd := exec.Command("curl", "--max-time", "30", p.Ip+":8090/"+code)

	log.Println("Get: calling", p.Ip+":8090/"+code)

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	fmt.Println(err)
	if err != nil {
		return "", err
	}
	s := out.String()
	return s, nil
}

func (x *Cluster) poll() {
	for id := range x.Nodes {
		go x.check(id)
	}
	log.Println("waiting for check to finish")
	time.Sleep(35 * time.Second)
}

func (x *Cluster) check(id string) {
	cou, e := Get(x.Nodes[id], "cou")
	node := x.Nodes[id]
	if e != nil {
		if node.Status != constants.Inactive {
			x.StatusHasChanged = true
		}
		node.Status = constants.Inactive
	} else {
		if node.Status != constants.Active {
			x.StatusHasChanged = true
		}
		node.Status = constants.Active
	}
	x.Nodes[id] = node
	log.Println(cou, e)
}

func (x *Cluster) Run(dir0 string, caip string, cbip string) {
	for {
		y, h, e := getNodesFromCA(caip)

		if e != nil {
			log.Println("CABC e=", e)
			time.Sleep(time.Second)
			continue
		}

		for id := range h {
			if h[id] != x.LastHashes[id] {
				x.BCHasChanged = true
			}
		}
		B := false
		if x.BCHasChanged {
			log.Println("CABC has changed")
			for id := range y {
				y[id].Status = constants.Active
			}
			x.Add(y)
			x.BCHasChanged = false
			x.LastHashes = h
			B = true
		}

		x.poll()
		if x.StatusHasChanged {
			log.Println("Staus has changed")
			x.Regenerate()
			x.StatusHasChanged = false
			B = true
		}
		if B {
			x.Submit(cbip)
			x.Persist(dir0)
		}

		log.Println("clusterat the moment", x.Strong(), "\nRM\n", x.Rou.RM)

	}
}

func (x *Cluster) CreateRoutes(id string) map[string][]string {

	if len(x.Rou.RM) <= 0 {
		log.Println("zero RM")
		return nil
	}

	var XCompressed = make([][]int, 0)
	var XPower = make([][][]int, 0)

	XCompressed = matri.Compress(x.Rou.RM)
	XPower = make([][][]int, 5)
	XPower[0] = x.Rou.RM

	for t := 1; t < 5; t++ {
		XPower[t] = matri.Times(x.Rou.RM, XPower[t-1])
	}

	var rs = mrand.NewSource(time.Now().UnixNano())
	var ra = mrand.New(rs)

	T0 := 2 // this is the number of paths i->j
	n := x.Nodes[id].N
	P := make(map[string]map[string]bool)
	for j := range x.Nodes {
		if x.Nodes[j].Status != constants.Active {
			continue
		}
		if j == id {
			continue
		}
		m := x.Nodes[j].N
		v := make(map[string]bool)
		for t := 0; t < T0; t++ {
			k := matri.Next(n, m, XCompressed, XPower, ra)

			if k >= len(x.Rou.Inv) {
				log.Println("Rou.Inv and XPower mismatch k=", k, "len(Inv)", len(x.Rou.Inv), "cluster.Inv", x.Rou.Inv, "rou.RM", x.Rou.RM)
				return nil
			}

			id1 := x.Rou.Inv[k]
			v[id1] = true
		}
		P[j] = v
	}
	Q := make(map[string][]string)
	for i := range P {
		w := make([]string, len(P[i]))
		c := 0
		for j := range P[i] {
			w[c] = j
			c += 1
		}
		Q[i] = w
	}
	return Q
}

func (x *Cluster) ImmediateContacts(id0 string) []map[int][][]string {
	V := make([]map[int][][]string, x.L)
	if len(x.RT.LayerMap) == x.L {
		for l := 0; l < x.L-1; l++ {
			V[l] = make(map[int][][]string)
			for n := range x.RT.LayerMap[l] {
				for i := range x.RT.LayerMap[l][n] {
					id := x.RT.LayerMap[l][n][i]
					if id == id0 {
						V[l][n] = x.RT.LayerMap[l+1][n*x.D : (n+1)*x.D]
					}
				}
			}
		}
	} else {
		log.Println("cluster: lenght mismatch", len(x.RT.LayerMap), x.L)
	}
	return V
}

func Convert(s []byte) string {
	y := sha256.Sum256((s))
	id := hex.EncodeToString(y[:])
	return id
}

func (x Cluster) Doc_Peer(doc_id string) []string {
	L := len(x.Cov.A)
	y := make([]string, 0)
	for l := 0; l < L; l++ {
		if len(x.Cov.A[l]) == 0 {
			continue
		}
		s := x.Doc_peer(doc_id, l)
		if s != "" {
			y = append(y, s)
		}
	}

	y1 := make([]string, 0)
	for i := 0; i < len(y); i++ {
		if x.Nodes[y[i]].Status == constants.Active {
			y1 = append(y1, y[i])
		}
	}

	return y1
}

func (x Cluster) Doc_peer(doc_id string, l int) string {
	return x.doc_peer(doc_id, l, 0, len(x.Cov.A[l])-1)
}

func (x Cluster) doc_peer(doc_id string, l int, i1 int, i2 int) string {
	i := (i1 + i2) / 2

	if i1 == i2 {
		id := x.Cov.A[l][i]
		if x.Nodes[id].I12 <= doc_id && doc_id < x.Nodes[id].I22 {
			return id
		} else {
			return ""
		}
	}
	if i2 == i1+1 {
		id := x.Cov.A[l][i1]
		if x.Nodes[id].I12 <= doc_id && doc_id < x.Nodes[id].I22 {
			return id
		}
		id = x.Cov.A[l][i2]
		if x.Nodes[id].I12 <= doc_id && doc_id < x.Nodes[id].I22 {
			return id
		}
		return ""
	}

	id1 := x.Cov.A[l][i]

	if doc_id < x.Nodes[id1].I12 {
		return x.doc_peer(doc_id, l, i1, i-1)
	}
	id2 := x.Cov.A[l][i]
	if doc_id >= x.Nodes[id2].I22 {
		return x.doc_peer(doc_id, l, i+1, i2)
	}
	id3 := x.Cov.A[l][i]
	if x.Nodes[id3].I12 <= doc_id && doc_id < x.Nodes[id3].I22 {
		return id3
	}
	id4 := x.Cov.A[l][i1]
	if x.Nodes[id4].I12 <= doc_id && doc_id < x.Nodes[id4].I22 {
		return id4
	}
	id5 := x.Cov.A[l][i2]
	if x.Nodes[id5].I12 <= doc_id && doc_id < x.Nodes[id5].I22 {
		return id5
	}
	panic("cluster: doc_peer unreachable")
}
