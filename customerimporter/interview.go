// package customerimporter reads from the given customers.csv file and returns a
// sorted (data structure of your choice) of email domains along with the number
// of customers with e-mail addresses for each domain.  Any errors should be
// logged (or handled). Performance matters (this is only ~3k lines, but *could*
// be 1m lines or run on a small machine).
package customerimporter

import (
	"encoding/csv"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Person struct {
	firstName string
	lastName  string
	email     string
	gender    string
	ipAddress string
}

type Domain map[string][]Person

var (
	// list of channels to communicate with workers
	// workers accessed synchronousely no mutex required
	workers = make(map[string]chan []string)

	// wg is to make sure all workers done before exiting main
	wg = sync.WaitGroup{}

	// mu used only for sequential writing
	mu = sync.Mutex{}

	domains = Domain{}
)

func Process(rec []string) {
	l := len(rec)
	part := rec[l-1]

	if c, ok := workers[part]; ok {
		// send rec to worker
		c <- rec
	} else {
		// if no worker for the part

		// make a chan
		nc := make(chan []string)
		workers[part] = nc

		// start worker with this chan
		go Worker(nc)

		// send rec to worker via chan
		nc <- rec
	}
}

func Worker(c chan []string) {
	// wg.Done signals to main worker completion
	wg.Add(1)
	defer wg.Done()

	doms := Domain{}
	for {
		// wait for a rec or close(chan)
		rec, ok := <-c
		if ok {
			// save the rec in structure
			email := rec[2]
			domIndex := strings.Index(email, "@")
			if domIndex != -1 {
				dom := email[domIndex:]
				per := Person{
					firstName: rec[0],
					lastName:  rec[1],
					email:     email,
					gender:    rec[3],
					ipAddress: rec[4],
				}
				doms[dom] = append(doms[dom], per)
			}
		} else {
			// channel closed on EOF

			// locks ensures sequential writing to Domains
			mu.Lock()
			for dom, pers := range doms {
				domains[dom] = append(domains[dom], pers...)
			}
			mu.Unlock()

			return
		}
	}
}

func CloseWorkers() {
	for _, c := range workers {
		// signal to all workers to exit
		close(c)
	}
}

func Insert(ss []string, s string) []string {
	i := sort.SearchStrings(ss, s)
	ss = append(ss, "")
	copy(ss[i+1:], ss[i:])
	ss[i] = s
	return ss
}

// write to a csv file
func WriteCSVOutput(w io.Writer, sortedDom []string) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	for _, dom := range sortedDom {
		for _, per := range domains[dom] {
			s := []string{
				per.firstName,
				per.lastName,
				per.email,
				per.gender,
				per.ipAddress,
			}
			err := writer.Write(s)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// write to a csv file
func WriteCSVDomainsCount(w io.Writer, sortedDom []string) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	for _, dom := range sortedDom {
		s := []string{
			dom,
			strconv.Itoa(len(domains[dom])),
		}
		err := writer.Write(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func Domains() Domain {
	return domains
}

func WaitGroup() *sync.WaitGroup {
	return &wg
}
