package node

import (
	"strconv"
	"time"
)

type Node struct {
	Name   string
	PubK   string
	Id     string
	Ip     string
	Port_h string // man
	Port_p string // peers: inbox/outbox
	Port_s string // leaf
	Port_r string // rtree
	Port_q string // qserver = root
	Port_g string // gateway
	I1     string
	I2     string
	I10    string
	I20    string
	I11    string
	I21    string
	I12    string
	I22    string

	Status string

	//	J12 big.Int
	//	J22 big.Int

	Pem string

	N int       // index in the routing table
	T time.Time // last update time
	D float64   // disk capacity, in GB
}

func (x Node) String() string {
	s := "Node: "
	s += "Id:" + x.Id + " "
	s += "Ip:" + x.Ip + " "
	s += "I12:" + x.I12 + " "
	s += "I22:" + x.I22 + " "
	s += "D:" + strconv.FormatFloat(x.D, 'E', -1, 64) + " "
	s += "Status:" + x.Status + "\n"
	//	s += "Pem" + x.Pem + "\n"
	return s
}
