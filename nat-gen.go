package main

import (
	"bytes"
	"database/sql"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/astaxie/beego/config"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"nat-gen/logs"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var exit chan bool
var datachan chan []byte
var log *logs.BeeLogger
var logdetail *logs.BeeLogger
var db *sql.DB

var serverport, cache, threadnum, cacheprint int
var serverip string

var execsql string
var format []string
var useattr []string
var xmlnode []string
var sqlnode []string
var recvin uint64
var temprecv uint64

func init() {
	//config
	iniconf, err := config.NewConfig("ini", "config.ini")
	if err != nil {
		panic(err.Error())
	}
	//serverinit
	serverip = iniconf.String("Server::ip")
	if serverip == "" {
		serverip = "0.0.0.0"
	}
	serverport, _ = iniconf.Int("Server::port")
	if serverport == 0 || serverport < 0 || serverport > 65535 {
		serverport = 3064
	}
	cache, _ = iniconf.Int("Server::cache")
	if cache == 0 || cache < 0 || cache > 3000000 {
		cache = 100000
	}
	datachan = make(chan []byte, cache)
	threadnum, _ = iniconf.Int("Server::threadnum")
	if threadnum == 0 || threadnum < 0 || threadnum > 30000 {
		threadnum = 10
	}
	//serverinit finish
	//log init
	logdir, err := os.Stat("log")
	if err != nil {
		err = os.Mkdir("log", 0777)
		if err != nil {
			panic(err)
		}
	} else {
		if logdir.IsDir() == false {
			err = os.Mkdir("log", 0777)
			if err != nil {
				panic(err)
			}
		}
	}
	logname := iniconf.String("Log::logname")
	if len(logname) == 0 {
		logname = "log/server.log"
	} else {
		logname = "log/" + logname
	}

	logsize, _ := iniconf.Int("Log::maxsize")
	if logsize == 0 {
		logsize = 500 * 1024 * 1024
	} else {
		logsize = logsize * 1024 * 1024
	}
	logsaveday, _ := iniconf.Int("Log::maxdays")
	if logsaveday == 0 {
		logsaveday = 7
	}
	loglevel, _ := iniconf.Int("Log::debug")
	if loglevel == 0 || loglevel < 0 || loglevel > 4 {
		loglevel = 1
	}
	cacheprint, _ = iniconf.Int("Log::cacheprint")
	if cacheprint == 0 || cacheprint < 0 {
		cacheprint = 10
	}

	log = logs.NewLogger(10000)
	log.SetLevel(loglevel)
	logstr := fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	log.SetLogger("file", logstr)
	logdetail = logs.NewLogger(10000)
	logdetail.SetLevel(1)
	logname = "log/logdetail.log"
	logstr = fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	logdetail.SetLogger("file", logstr)
	//loginit finish
	//dbinit
	dbhost := iniconf.String("Db::dbhost")
	dbport := iniconf.String("Db::dbport")
	dbuser := iniconf.String("Db::dbuser")
	dbpassword := iniconf.String("Db::dbpassword")
	dbname := iniconf.String("Db::dbname")
	dsn := dbuser + ":" + dbpassword + "@tcp(" + dbhost + ":" + dbport + ")/" + dbname + "?charset=utf8&loc=Asia%2FShanghai"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	db.SetMaxIdleConns(500)
	db.SetMaxOpenConns(500)
	var dbisok = make(chan bool, 0)
	go func() {
		var isok bool
		for i := 0; i < 5; i++ {
			fmt.Println("start to pin")
			err = db.Ping()
			if err != nil {
				fmt.Printf("Db connect error,rety 10 seconds after, now rety %d count\n", i+1)
				time.Sleep(time.Second * 1)
				continue
			}
			isok = true
		}
		dbisok <- isok
	}()
	isok := <-dbisok
	if isok == false {
		fmt.Println("Db not connect,please check db!")
		os.Exit(2)
	} else {
		fmt.Println("Db connect.")
	}
	//dbinit finish
	//execinit
	execsql = iniconf.String("Exec::sql")
	sqlnode = FormatSql(execsql)
	xmlnode, err = LoadXmlNode("./nat-gen.xml")
	for _, v := range sqlnode {
		fmt.Println(v)
	}
	fmt.Println("**************")
	for _, v := range xmlnode {
		fmt.Println(v)
	}
	fmt.Println("**************")
	if err != nil {
		fmt.Println("load nat-gen.xml err.")
		os.Exit(2)
	}
	//useattr = regexp.MustCompile(`{.*?}`).FindAllString(execsql, -1)
	//execinit
}

func main() {
	addr, _ := net.ResolveUDPAddr("udp", serverip+":"+strconv.Itoa(serverport))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	log.Info("started.")
	var b [512]byte
	//启动监听线程
	go func() {
		for {
			n, _ := conn.Read(b[:])
			p := b[:n]
			recvin++
			datachan <- p
		}
	}()
	//启动对应数量线程

	for i := 0; i < threadnum; i++ {

		go WriteSysLog(i)
	}
	//启动统计线程
	ticker := time.NewTicker(time.Duration(cacheprint) * time.Second)
	go func() {
		for _ = range ticker.C {
			temp := (recvin - temprecv) / uint64(cacheprint)
			log.Info("now the cache is %d,recv in is %d,speed is %d.", cache-len(datachan), recvin, temp)
			temprecv = recvin
		}
	}()

	<-exit
}

//Decode is decode the byte to syslog
func Decode(s []byte) map[string]string {
	data := make(map[string]string)
	str := string(s)
	log.Debug(str)
	data["{deviceip}"] = regexp.MustCompile(`((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`).FindString(str)
	log.Debug("deviceip is %s", data["{deviceip}"])
	data["{timestamp}"] = regexp.MustCompile(`(\d{4})-(0\d{1}|1[0-2])-(0\d{1}|[12]\d{1}|3[01])T(0\d{1}|1\d{1}|2[0-3]):[0-5]\d{1}:([0-5]\d{1})\.\d{1,}Z`).FindString(str)
	log.Debug("timestamp is %s", data["{timestamp}"])
	portrangetype := regexp.MustCompile(`An.*?portrange\s`).FindString(str)
	data["{portrangetype}"] = regexp.MustCompile(`(An\s|\sportrange\s)`).ReplaceAllString(portrangetype, ``)
	log.Debug("portrangetype is %s", data["{portrangetype}"])
	portaction := regexp.MustCompile(`is.*?,`).FindString(str)
	data["{portaction}"] = regexp.MustCompile(`(is\s|,)`).ReplaceAllString(portaction, ``)
	log.Debug("portaction is %s", data["{portaction}"])
	privateip := regexp.MustCompile(`privateip='.*?'`).FindString(str)
	data["{privateip}"] = regexp.MustCompile(`(privateip='|')`).ReplaceAllString(privateip, ``)
	log.Debug("privateip is %s", data["{privateip}"])
	srcvrfid := regexp.MustCompile(`srcvrfid='.*?'`).FindString(str)
	data["{srcvrfid}"] = regexp.MustCompile(`(srcvrfid='|')`).ReplaceAllString(srcvrfid, ``)
	log.Debug("srcvrfid is %s", data["{srcvrfid}"])
	publicip := regexp.MustCompile(`publicip='.*?'`).FindString(str)
	data["{publicip}"] = regexp.MustCompile(`(publicip='|')`).ReplaceAllString(publicip, ``)
	log.Debug("publicip is %s", data["{publicip}"])
	publicstartport := regexp.MustCompile(`publicportrange='\d*?~`).FindString(str)
	data["{publicstartport}"] = regexp.MustCompile(`(publicportrange='|~)`).ReplaceAllString(publicstartport, ``)
	log.Debug("publicstartport is %s", data["{publicstartport}"])
	publicsendport := regexp.MustCompile(`~\d*?'`).FindString(str)
	data["{publicsendport}"] = regexp.MustCompile(`('|~)`).ReplaceAllString(publicsendport, ``)
	log.Debug("publicsendport is %s", data["{publicsendport}"])
	actiontime := regexp.MustCompile(`time='.*?'`).FindString(str)
	data["{actiontime}"] = regexp.MustCompile(`(time='|')`).ReplaceAllString(actiontime, ``)
	log.Debug("actiontime is %s", data["{actiontime}"])
	isval := Checknil(data)
	if isval == false {
		log.Info("data format error! the value is not enough.")
		return make(map[string]string)
	}
	return data
}

//Checknil check the map val is not nil
func Checknil(m map[string]string) bool {
	for _, v := range useattr {
		if len(m[v]) == 0 {
			log.Info("%s is not null!", v)
			return false
		}
	}
	return true
}
func LoadXmlNode(filename string) (xmlnode []string, err error) {
	xmlfile, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	xmldecode := xml.NewDecoder(bytes.NewBuffer(xmlfile))
	var attrname string
	for t, err := xmldecode.Token(); err == nil; t, err = xmldecode.Token() {
		switch se := t.(type) {
		case xml.StartElement:
			attrname = se.Name.Local
		case xml.CharData:
			content := string([]byte(se))
			if content[0] != 15 && content[0] != 40 && content[0] != 12 && content[0] != 10 {
				xmlnode = append(xmlnode, strings.ToUpper(attrname))

			}
		case xml.EndElement:
		default:
		}
	}
	return
}
func FormatSql(sql string) (sqlnode []string) {
	sql = strings.ToUpper(sql)
	sqlnode = strings.Split(sql, " ")
	return
}
func DecodeSyslog(logstr string, xmlnode []string) (lognode map[string]string, err error) {
	logstr = strings.ToUpper(logstr)
	logstr = strings.Replace(logstr, ";", "", -1)
	logstr = strings.Replace(logstr, "[", "", -1)
	logstr = strings.Replace(logstr, "]", "", -1)
	logstr = strings.Replace(logstr, "-->", "", -1)
	logstr = strings.Replace(logstr, "-DEVIP=", "", -1)
	logstr = strings.Replace(logstr, "PROTOCOL:", "", -1)
	node := strings.Split(logstr, " ")
	if len(node) != len(xmlnode) {
		err = errors.New("syslog format err.")
		return
	}
	lognode = make(map[string]string)
	for index, v := range xmlnode {
		lognode[v] = node[index]
	}

	return
}
func WriteToFile(lognode map[string]string, sqlnode []string) (result string) {
	for _, n := range sqlnode {
		v, ok := lognode[n]
		if ok {
			result += v + " "
		} else {
			result += " "
		}
	}
	return
}
func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func WriteSysLog(index int) {
	var f *os.File
	filename := fmt.Sprintf("log/logdetail%d.log", index)
	fmt.Println(filename)
	if checkFileIsExist(filename) {
		f, _ = os.OpenFile(filename, os.O_APPEND, 0666)
	} else {
		f, _ = os.Create(filename)
	}
	for {
		rawdata := <-datachan
		str := string(rawdata)
		lognode, err := DecodeSyslog(str, xmlnode)
		if err != nil {
			log.Debug("%s|%s", str, err.Error())
			continue
		}
		str = WriteToFile(lognode, sqlnode)
		io.WriteString(f, str)
		//logdetail.Info(str)

		/*	data := Decode(rawdata)
			if len(data) == 0 {
				return
			}
			var exec = execsql
			for _, v := range useattr {
				exec = strings.Replace(exec, v, data[v], -1)
			}
			log.Debug(exec)
			row := db.QueryRow(exec)
			if err != nil {
				log.Info(err.Error())
			} else {
				log.Info("insert db success.")
			}
			var result int
			err := row.Scan(&result)
			if err != nil {
				log.Info(err.Error())
			} else {
				log.Info(exec + "| result:" + strconv.Itoa(result))
			}
		*/

	}
}