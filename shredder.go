package main

// this code is responsible for processing the encountered urls.
// we look up the url in a DB, and it it does not exist, we add it , and also add the corresponding host to be active

import (
	"bufio"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"time"

	"ouroboros/src/murl"
	utils "ouroboros/src/utils"

	"github.com/pkg/profile"
)

type Conf struct {
	Dir0 string
}

var conf Conf

var rs = rand.NewSource(time.Now().UnixNano())
var ra = rand.New(rs)

var WI = make(chan string, 1000)

var tot = 0

var symbol []int

func Proc(fname string) {
	f, e := os.OpenFile(fname, os.O_RDONLY, 0644)
	if e != nil {
		log.Println("cannot open file ", e)
		return
	}
	b := bufio.NewReader(f)
	for {
		s, e := b.ReadBytes('\n')
		if e != nil {
			break
		}
		if len(s) <= 0 {
			continue
		}
		s1 := s[0 : len(s)-1]
		h := murl.Ho(string(s1))
	LOCK:
		if _, err := os.Stat(conf.Dir0 + "/tmp_processor/" + h + ".lock"); err == nil {
			log.Println("file is locked ", h)
			time.Sleep(time.Duration(1e+7))
			goto LOCK
		}
		g, e := os.OpenFile(conf.Dir0+"/tmp_processor/"+h, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		g.Write(s)
		g.Close()
	}
	f.Close()
	os.Remove(fname)
}

func List() {
	f, _ := os.OpenFile(conf.Dir0+"/link_proc.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	cmd := exec.Command("ls", conf.Dir0+"/tmp_crawler_links1")
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	cmd.Run()
	f.Close()
}

func count() int {
	f, e := os.OpenFile(conf.Dir0+"/link_proc.txt", os.O_RDONLY, 0644)
	if e != nil {
		return 0
	}
	b := bufio.NewReader(f)
	cou := 0
	for {
		_, e := b.ReadString('\n')
		if e != nil {
			break
		}
		cou += 1
	}
	return cou
}

func Run() {
	for {
		List()
		nfiles := count()
		ioutil.WriteFile(conf.Dir0+"/nfiles_to_process.txt", []byte(strconv.Itoa(nfiles)), 0644)
		log.Println("nfiles=", nfiles)
		f, e := os.OpenFile(conf.Dir0+"/link_proc.txt", os.O_RDONLY, 0644)
		if e != nil {
			log.Println("no link_proc file")
			time.Sleep(time.Second)
			continue
		}
		fi, _ := f.Stat()
		if fi.Size() == 0 {
			log.Println("zero file")
			f.Close()
			time.Sleep(time.Second)
			continue
		}
		b := bufio.NewReader(f)
		cou := 0
		for {
			fn, e := b.ReadString('\n')
			if e != nil {
				time.Sleep(time.Duration(1e+9))
				break
			}
			if len(fn) <= 1 {
				continue
			}
			fn = fn[0 : len(fn)-1]
			cou += 1
			Proc(conf.Dir0 + "/tmp_crawler_links1/" + fn)
		}
		f.Close()
		os.Remove(conf.Dir0 + "/link_proc.txt")
		if cou == 0 {
			time.Sleep(time.Second)
		}
		log.Println("cou", cou)
	}
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
	utils.MakeDir0(conf.Dir0 + "/tmp_processor")
	utils.MakeDir0(conf.Dir0 + "/tmp_processor1")

	utils.MakeDir0(conf.Dir0 + "/hostsd")
	utils.MakeDir0(conf.Dir0 + "/tmp_crawler_links")
	utils.MakeDir0(conf.Dir0 + "/tmp_crawler_links1")

	for i := 0; i < 256; i++ {
		symbol = append(symbol, i)
	}

	Run()
}
