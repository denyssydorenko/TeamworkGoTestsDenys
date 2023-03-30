package customerimporter

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var csvTest = strings.NewReader(`
first_name,last_name,email,gender,ip_address
denys,sydorenko,denys.sydorenko1997@gmail.com,Male,192.168.1.1
olga,sydorenko,olga.sydorenko@gmail.com,Female,192.168.1.1
david,gallardo,david.gallardo@outlook.com,Male,192.168.1.1`)

var expectedDomainResp = Domain{
	"@gmail.com": {
		{
			firstName: "denys",
			lastName:  "sydorenko",
			email:     "denys.sydorenko1997@gmail.com",
			gender:    "Male",
			ipAddress: "192.168.1.1",
		},
		{
			firstName: "olga",
			lastName:  "sydorenko",
			email:     "olga.sydorenko@gmail.com",
			gender:    "Female",
			ipAddress: "192.168.1.1",
		},
	},
	"@outlook.com": {
		{
			firstName: "david",
			lastName:  "gallardo",
			email:     "david.gallardo@outlook.com",
			gender:    "Male",
			ipAddress: "192.168.1.1",
		},
	},
}

func Test_ReadCSVFile(t *testing.T) {
	wg := WaitGroup()
	r := csv.NewReader(csvTest)
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				CloseWorkers()
				break
			}
		}
		Process(rec)
	}
	wg.Wait()
	persLenG := lenByDomain("@gmail.com")
	persLenO := lenByDomain("@outlook.com")
	assert.Equal(t, 2, persLenG, fmt.Sprintf("Expected result count for gmail %d, received %d", 2, persLenG))
	assert.Equal(t, 1, persLenO, fmt.Sprintf("Expected result count for outlook %d, received %d", 1, persLenO))
	assert.Equal(t, expectedDomainResp, Domains())
}

func lenByDomain(dom string) int {
	return len(Domains()[dom])
}
