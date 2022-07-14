package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"ouroboros/src/load_conf"
	node "ouroboros/src/node"
	"time"
)

type Cover struct {
	Byl [][]string
}

type Interv struct {
	I1 string
	I2 string
}

type Roles struct {
	Id string
	R  map[int][]Interv
}

type Conf struct {
	Dir0  string
	Me    node.Node
	Nodes map[string]node.Node
}

var conf Conf

func Configure(dname string) {
	conf.Dir0 = dname
	load_conf.LoadWaitInterface(dname+"/conf/me.txt", &conf.Me)
	load_conf.LoadWaitInterface(dname+"/net/nodes.txt", &conf.Nodes)
}

func Get(p node.Node, code string) (string, error) {
	cmd := exec.Command("curl", "--max-time", "30", p.Ip+":8090/"+code)
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

func poll() {
	for id := range conf.Nodes {
		go check(id)
	}
}

func check(id string) {

	log.Println("id=", id)

	cou, e := Get(conf.Nodes[id], "cou")

	log.Println("cou,e", cou, e)

	node := conf.Nodes[id]
	if e != nil {
		node.Status = "inactive"
	} else {
		node.Status = "active"
	}
	conf.Nodes[id] = node
	log.Println(cou, e)
}

func Cycle() {
	for {
		poll()
		log.Println("collecting poll data")
		time.Sleep(60 * time.Second)
		s := ""
		for id := range conf.Nodes {
			s += id + " : " + conf.Nodes[id].Status + "\n"
		}

		log.Println("s=", s)

		ioutil.WriteFile(conf.Dir0+"/cluster_status.txt", []byte(s), 0644)
		time.Sleep(10 * 60 * time.Second)
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

	Configure(dir0)

	Cycle()
}
