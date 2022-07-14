package fetcher

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	//"fmt"
	"compress/gzip"
	"io"

	links "ouroboros/src/links"
	simple "ouroboros/src/simple"
)

func One(client *http.Client, name string, to string) (map[string]bool, error) {
	w := make(map[string]bool)
	req, err := http.NewRequest("GET", name, nil)
	if err != nil {
		fmt.Println("One err=", err)
		return w, err
	}
	moz := "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"

	req.Header.Set("User-Agent", moz)
	resp, err := client.Do(req)
	if err != nil {
		return w, err
	}
	defer resp.Body.Close()
	var reader io.Reader
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return w, err
		}
		//		defer reader.Close()
	default:
		reader = resp.Body
		//		defer reader.Close()
	}
	if reader == nil {
		return w, err
	}
	typ := resp.Header.Get("Content-Type")
	if typ == "application/pdf" {
		return w, err
	}
	if typ == "image/jpeg" {
		return w, err
	}
	if strings.Index(typ, "text/html") == -1 && strings.Index(typ, "gzip") == -1 {
		return w, err
	}

	x0 := simple.Init([]byte("<!--"), []byte("-->"))
	x0.SetReader(reader)

	x1 := simple.Init([]byte("<script"), []byte("</script>"))
	x1.SetReader(x0)

	x2 := simple.Init([]byte("<style"), []byte("</style>"))
	x2.SetReader(x1)

	x21 := simple.Init([]byte("&lt;style"), []byte("&lt;/style&gt;"))
	x21.SetReader(x2)

	var y links.Links
	y.SetReader(x21)

	/*
		f, _ := os.OpenFile("ooo.txt", os.O_CREATE|os.O_WRONLY, 0644)
		f.Truncate(0)
		defer f.Close()
		y.SetWriter(io.Writer(f))
	*/

	buf := new(bytes.Buffer)
	y.SetWriter(buf)
	/*
		x3 := bracket.Init()
		x3.SetReader(&y)

		x4 := space.Init()
		x4.SetReader(x3)

		x5 := filter.Init()
		x5.SetReader(x4)

		//	id := utils.Url_Id(name)
		g, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return w, err
		}
		defer g.Close()
		g.Truncate(0)

		//	fmt.Println("fetcher:", name)
		n1, e1 := io.Copy(g, x5)

		if e1 != nil {
			//	fmt.Println("n1,e1", n1, e1)
			_ = n1
		}
	*/

	io.Copy(ioutil.Discard, &y)

	u0, _ := url.Parse(name)

	host0 := u0.Host
	scheme0 := u0.Scheme
	var scheme, host, path, query string

	for {
		s, e := buf.ReadBytes('\n')
		if e != nil {
			break
		}
		//		x := strings.TrimSpace(string(s))
		x := string(s)
		if len(x) <= 2 {
			continue
		}
		x = x[0 : len(x)-1]

		for len(x) > 0 && (x[len(x)-1] == ' ' || x[len(x)-1] == '\n' || x[len(x)-1] == '\r' || x[len(x)-1] == '\t') {
			x = x[0 : len(x)-1]
		}

		for len(x) >= 1 && (x[0] == ' ' || x[0] == '\n' || x[0] == '\r' || x[0] == '\t') {
			x = x[1:]
		}

		u, e := url.Parse(x)
		if e == nil {
			if u.Scheme == "" {
				scheme = scheme0
			} else {
				scheme = u.Scheme
			}
			if u.Scheme != "" && u.Scheme != "http" && u.Scheme != "https" {
				continue
			}

			if u.Host == "" {
				host = host0
			} else {
				host = u.Host
			}

			path = u.Path
			if len(path) >= 1 && path[0] != '/' {
				path = "/" + path
			}

			query = u.RawQuery
			if len(query) >= 1 && query[0] != '/' {
				query = "/" + query
			}

			v := scheme + "://" + host + path + query

			w[v] = true

			//			fmt.Println(x, "    scheme=", u.Scheme, "    host=", u.Host, "   path=", u.Path, "    query", u.Query(), "   rawquery=", u.RawQuery)
			//			fmt.Println(v)
		} else {
			//	fmt.Println("fetcher parse url error", x, e, "name=\n", name)
		}
		//		fmt.Println()
	}

	return w, nil

}
