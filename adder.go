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

	btma_prefix "ouroboros/src/btma_prefix"
	utils "ouroboros/src/utils"

	"ouroboros/src/btms"
	btr "ouroboros/src/btr"

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

var COU = int64(0)

func OneHost() {

	y := btr.New()

BEG:
	hosts := btma_prefix.OpenOrCreate(ra, conf.Dir0, "hostsd/host.txt")
	if hosts == nil {
		time.Sleep(time.Second)
		goto BEG
	}

	hlist, e := os.OpenFile(conf.Dir0+"/host_list.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	// redundant dile, for convenience
	if e != nil {
		log.Fatal(e)
	}

	cou := 0
	for {

		h := <-WI
		//MO:

		f0, e := os.OpenFile(conf.Dir0+"/tmp_processor/"+h, os.O_RDONLY, 0644)

		if e != nil {
			log.Println("error opening processor ", e)
			time.Sleep(time.Second)
			continue
		}

		fi, _ := f0.Stat()
		if fi.Size() <= 0 {
			log.Println("empty file ", h)
			f0.Close()
			continue
		}
		f0.Close()

		e = os.Symlink(conf.Dir0+"/tmp_processor/"+h, conf.Dir0+"/tmp_processor/"+h+".lock")
		if e != nil {
			log.Println("symlink e", e)
			time.Sleep(time.Second)
			continue
		}
		e1 := os.Rename(conf.Dir0+"/tmp_processor/"+h, conf.Dir0+"/tmp_processor1/"+h)
		os.Remove(conf.Dir0 + "/tmp_processor/" + h + ".lock")
		if e1 != nil {
			log.Println("error renaming", e1)
		}

	MID:
		f, e2 := os.OpenFile(conf.Dir0+"/tmp_processor1/"+h, os.O_RDONLY, 0644)
		if e2 != nil {
			log.Println("open e=", e2)
			time.Sleep(time.Duration(1e+8))
			goto MID
		}
		hf := btms.OpenOrCreate(nil, conf.Dir0+"/hosts", h)
		uf, _ := os.OpenFile(conf.Dir0+"/resou/"+h, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

		b := bufio.NewReader(f)
		c := 0
		for {
			s, e := b.ReadString('\n')
			if e != nil {
				break
			}
			s = s[:len(s)-1]
			n := hf.Add(0, s)
			if n >= 0 {
				uf.Write([]byte(s + "\n"))
				c += 1
				COU += 1
			}
		}
		uf.Close()
		hf.Close()
		if c > 0 {
			n := y.Add(h)
			if n != nil {
				hosts.Add(0, h)
				hlist.Write([]byte(h + "\n"))
			}
		}
		cou += 1
		os.Remove(conf.Dir0 + "/tmp_processor1/" + h)

		if cou%1000 == 0 {
			log.Println(cou)
		}
	}
}

func List() {
	f, _ := os.OpenFile(conf.Dir0+"/tmp_processor.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	cmd := exec.Command("ls", conf.Dir0+"/tmp_processor")
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	cmd.Run()
	f.Close()
}

func count() int {
	f, e := os.OpenFile(conf.Dir0+"/tmp_processor.txt", os.O_RDONLY, 0644)
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
	utils.MakeDir0(conf.Dir0 + "/old_tmp_proc")
	for {
		List()
		nfiles := count()
		ioutil.WriteFile(conf.Dir0+"/nhosts_to_process.txt", []byte(strconv.Itoa(nfiles)), 0644)
		log.Println("nfiles=", nfiles)
		f, e := os.OpenFile(conf.Dir0+"/tmp_processor.txt", os.O_RDONLY, 0644)
		if e != nil {
			log.Println("no link_proc file")
			time.Sleep(time.Second)
			continue
		}
		fi, _ := f.Stat()
		if fi.Size() == 0 {
			log.Println("zero file COU", COU)
			f.Close()
			time.Sleep(3 * time.Second)
			continue
		}
		b := bufio.NewReader(f)
		cou := 0
		for {
			fn, e := b.ReadString('\n')
			if e != nil {
				break
			}

			//log.Println("fn=", fn)

			WI <- fn[0 : len(fn)-1]
			cou += 1
		}
		f.Close()
		os.Rename(conf.Dir0+"/tmp_processor.txt", conf.Dir0+"/old_tmp_proc/tmp_processor_"+strconv.FormatInt(time.Now().UnixNano(), 10)+".txt")
		log.Println("one iteration done, cou=", cou)
		time.Sleep(time.Second)
	}
}

func DeleteLocks() {
	ff, _ := ioutil.ReadDir(conf.Dir0 + "/tmp_processor")
	for _, f := range ff {
		s := f.Name()
		if len(s) >= 5 && s[len(s)-5:len(s)] == ".lock" {
			os.Remove(conf.Dir0 + "/tmp_processor/" + s)
		}
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

	DeleteLocks()

	for i := 0; i < 256; i++ {
		symbol = append(symbol, i)
	}

	go OneHost()
	Run()
}
