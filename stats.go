package main

// this process provides service to API calls regarding basic staistics of node operation

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"

	utils "ouroboros/src/utils"
)

type Conf struct {
	Dir0 string
}

var conf Conf

func listen() {
	log.Println("listen for peers", ":9001")
	ln, e := net.Listen("tcp", ":9001")
	utils.Check(e, "listening error")
	for {
		conn, _ := ln.Accept()
		fmt.Println("inbox: got connection")
		go talk(conn)
	}
}

// this decribes communication protocol regarding possible requests

func talk(conn net.Conn) {
	r := bufio.NewReader(conn)

	for {
		s, e := r.ReadBytes(0x11)
		if e != nil {
			log.Println("closing connection")
			conn.Close()
			return
		}
		s = s[0 : len(s)-1]

		log.Println("received", string(s))

		if string(s) == "list" {
			fs, _ := ioutil.ReadDir(conf.Dir0 + "/resou")
			n := 0
			for i := range fs {
				fn := fs[i].Name()
				if len(fn) < 4 || fn[len(fn)-4:] != ".poi" {
					n += len(fn) + 1
				}
			}
			ns := strconv.Itoa(n)
			conn.Write([]byte(ns))
			conn.Write([]byte{0x11})

			for i := range fs {
				fn := fs[i].Name()
				if len(fn) < 4 || fn[len(fn)-4:len(fn)] != ".poi" {
					conn.Write([]byte(fn + string(0x13)))
				}
			}
			conn.Write([]byte{0x11})
		}
		if string(s[0:4]) == "host" {
			name := string(s[4 : len(s)-1])
			f, e := os.OpenFile(conf.Dir0+"/resou/"+name, os.O_RDONLY, 0644)
			if e != nil {
				log.Println("error::", e)
				conn.Write([]byte("does not exist"))
				conn.Write([]byte{0x11})
				continue
			}

			u := bufio.NewReader(f)

			for {
				w, e := u.ReadBytes('\n')
				if e != nil {
					log.Println("red ", e)
					break
				}
				if len(w) < 1 {
					continue
				}

				conn.Write([]byte(w[0 : len(w)-1]))
				conn.Write([]byte{0x13})
			}
			conn.Write([]byte{0x11})
			conn.Write([]byte{0x13})
			f.Close()

		}
		if string(s) == "tar" {
			utils.MakeDir0(conf.Dir0 + "/tar")
			cmd := exec.Command("tar", "-czvf", conf.Dir0+"/tar/tar.tar", conf.Dir0+"/resou")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			if err != nil {
				log.Println("error tar", err)
			}
			f, _ := os.OpenFile(conf.Dir0+"/tar/tar.tar", os.O_RDONLY, 0644)
			b := bufio.NewReader(f)
			buf := make([]byte, 4096)
			for {
				n, e := b.Read(buf)
				conn.Write(buf[0:n])
				if e != nil {
					log.Println("read error", e)
					break
				}
			}
			f.Close()
			conn.Write([]byte{0x11})
			conn.Close()
		}
	}
}

func main() {

	if len(os.Args) > 1 {
		conf.Dir0 = os.Args[1]
	} else {
		conf.Dir0 = "/d"
	}

	utils.MakeDir0(conf.Dir0 + "/tar")

	listen()
}
