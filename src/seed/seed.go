package seed

import (
	//"fmt"
	//"os"

	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"ouroboros/src/utils"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func Seed(jobs chan string, dir0 string, I1 string, I2 string) {
BEG:
	r, e1 := ioutil.ReadFile(dir0 + "/seed/seed.txt")

	if e1 != nil || len(r) < 1000 {
		log.Println("lenr", len(r), "e1", e1)
		time.Sleep(time.Second)
		goto BEG
	}

	q := strings.Split(string(r), "\n")
	log.Println("seed", len(q))

	if len(q) <= 0 {
		log.Println("empty seed")
		os.Exit(0)
	}

	//	M := len(q)
	//	M0 := 10000
	c := 0
	c1 := 0

	i := 0

	for ; c < 9000; i++ {
		if len(q[i]) > 0 {

			s := "http://www." + q[i]

			id := utils.Url_Id(s)
			if I1 <= id && id < I2 {
				jobs <- s
				c += 1
			} else {
				c1 += 1
				//log.Println("not mine")
			}
		}
	}

	log.Println("done seed, exitting")

	g, e := os.OpenFile(dir0+"/queue.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if e != nil {
		log.Println("cannot open quefile", e)
	}
	defer g.Close()
	for ; i < len(q); i++ {
		if len(q[i]) > 0 {

			s := "http://" + q[i]

			id := utils.Url_Id(s)
			if I1 <= id && id < I2 {
				g.Write([]byte(s + "\n"))
				c += 1
			} else {
				c1 += 1
				log.Println("not mine")
			}
		}
	}

	log.Println("seeded with", c, "c1=", c1)

}
