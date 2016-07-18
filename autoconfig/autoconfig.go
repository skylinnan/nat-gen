package autoconfig

import (
	"crypto/md5"
	"io"
	"os"
)

type AutoConfig struct {
	oldMd5 []byte
	newMd5 []byte
	reload bool
}

func (a *AutoConfig) Load(filepath string) {
	file, err := os.Open(filepath)
	if err == nil {
		md5f := md5.New()
		io.Copy(md5f, file)
		a.newMd5 = md5f.Sum([]byte(""))
		if !a.Compare() {
			a.reload = true
		} else {
			a.reload = false
		}
	}
	a.oldMd5 = a.newMd5
	//fmt.Println(3 * time.Second)
	//time.Sleep(3 * time.Second)
}

func (a *AutoConfig) IsReload() bool {
	return a.reload
}

func (a *AutoConfig) Compare() bool {

	if len(a.oldMd5) != len(a.newMd5) {
		return false
	}

	for index, v := range a.oldMd5 {
		if v != a.newMd5[index] {
			return false
		}
	}
	return true
}
