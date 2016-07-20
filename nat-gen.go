package main

import (
	"bytes"
	"database/sql"
	"encoding/xml"
	"errors"
	//"flag"
	"fmt"
	"github.com/astaxie/beego/config"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"log"
	"nat-gen/logs"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	//"runtime/pprof"
	"nat-gen/autoconfig"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SessionInfo struct {
	info  string
	atime string
	wtime string
}

var f *os.File
var err error
var exit chan bool
var datachan chan []byte
var writedata chan string
var logfile *logs.BeeLogger
var logdetail *logs.BeeLogger
var db *sql.DB
var filename string
var fileindex uint64
var tmpindex uint64
var count uint64
var timecount int64
var serverport, cache, threadnum, cacheprint, filecount, filetimespan int
var serverip string
var profileserver string
var logflag string

var execsql string
var format []string
var useattr []string
var xmlnode []string
var sqlnode []string
var recvin uint64
var temprecv uint64
var mutex sync.Mutex
var userinfo map[string]SessionInfo
var natconfig *autoconfig.AutoConfig

//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
	filecount, _ = iniconf.Int("Server::filecount")
	if filecount == 0 {
		filecount = 10000
	}
	filetimespan, _ = iniconf.Int("Server::filetimespan")
	if filetimespan == 0 {
		filetimespan = 15
	}
	datachan = make(chan []byte, cache)
	writedata = make(chan string, cache)
	threadnum, _ = iniconf.Int("Server::threadnum")
	if threadnum == 0 || threadnum < 0 || threadnum > 30000 {
		threadnum = 10
	}
	profileserver = iniconf.String("Server::profileserver")
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
	logflag = iniconf.String("Log::logdetail")
	logfile = logs.NewLogger(10000)
	logfile.SetLevel(loglevel)
	logstr := fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	logfile.SetLogger("file", logstr)
	logdetail = logs.NewLogger(10000)
	logdetail.SetLevel(1)
	logname = "log/logdetail.log"
	logstr = fmt.Sprintf(`{"filename":"%s","maxsize":%d,"maxdays":%d}`, logname, logsize, logsaveday)
	logdetail.SetLogger("file", logstr)
	userinfo = make(map[string]SessionInfo)
	//loginit finish
	//dbinit
	/*dbhost := iniconf.String("Db::dbhost")
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
	*/
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

	natconfig = new(autoconfig.AutoConfig)

}
func InitConfig() {
	for {
		natconfig.Load("config.ini")
		if natconfig.IsReload() {
			iniconf, err := config.NewConfig("ini", "config.ini")
			if err != nil {
				panic(err.Error())
			}

			loglevel, _ := iniconf.Int("Log::debug")
			if loglevel == 0 || loglevel < 0 || loglevel > 4 {
				loglevel = 1
			}
			logfile.SetLevel(loglevel)
			logflag = iniconf.String("Log::logdetail")
			filecount, _ = iniconf.Int("Server::filecount")
			filetimespan, _ = iniconf.Int("Server::filetimespan")
			logfile.Info("reload config success .")
		}
		time.Sleep(3 * time.Second)
	}

}
func main() {
	go func() {
		log.Println(http.ListenAndServe(profileserver, nil))
	}()
	addr, _ := net.ResolveUDPAddr("udp", serverip+":"+strconv.Itoa(serverport))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	logfile.Info("started.")
	var b [512]byte
	//启动监听线程
	go func() {
		for {
			n, _ := conn.Read(b[:])
			p := b[:n]
			recvin++
			if logflag == "true" {
				logdetail.Info(string(p))
			}
			datachan <- p
		}
	}()
	//启动对应数量线程

	for i := 0; i < threadnum; i++ {

		go WriteSysLog()
	}
	//启动统计线程
	ticker := time.NewTicker(time.Duration(cacheprint) * time.Second)
	go func() {
		for _ = range ticker.C {
			temp := (recvin - temprecv) / uint64(cacheprint)
			logfile.Info("now the recv cache is %d,write data cache is %d,recv in is %d,speed is %d.", cache-len(datachan), cache-len(writedata), recvin, temp)
			temprecv = recvin
		}
	}()
	//启动日志文件记录线程
	filename = fmt.Sprintf("log/%s_%d.log", time.Now().Format("20060102150405"), fileindex)
	f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err.Error())
	}
	go WriteToFile()
	go InitConfig()
	<-exit
}

//Decode is decode the byte to syslog
func Decode(s []byte) map[string]string {
	data := make(map[string]string)
	str := string(s)
	logfile.Debug(str)
	data["{deviceip}"] = regexp.MustCompile(`((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`).FindString(str)
	logfile.Debug("deviceip is %s", data["{deviceip}"])
	data["{timestamp}"] = regexp.MustCompile(`(\d{4})-(0\d{1}|1[0-2])-(0\d{1}|[12]\d{1}|3[01])T(0\d{1}|1\d{1}|2[0-3]):[0-5]\d{1}:([0-5]\d{1})\.\d{1,}Z`).FindString(str)
	logfile.Debug("timestamp is %s", data["{timestamp}"])
	portrangetype := regexp.MustCompile(`An.*?portrange\s`).FindString(str)
	data["{portrangetype}"] = regexp.MustCompile(`(An\s|\sportrange\s)`).ReplaceAllString(portrangetype, ``)
	logfile.Debug("portrangetype is %s", data["{portrangetype}"])
	portaction := regexp.MustCompile(`is.*?,`).FindString(str)
	data["{portaction}"] = regexp.MustCompile(`(is\s|,)`).ReplaceAllString(portaction, ``)
	logfile.Debug("portaction is %s", data["{portaction}"])
	privateip := regexp.MustCompile(`privateip='.*?'`).FindString(str)
	data["{privateip}"] = regexp.MustCompile(`(privateip='|')`).ReplaceAllString(privateip, ``)
	logfile.Debug("privateip is %s", data["{privateip}"])
	srcvrfid := regexp.MustCompile(`srcvrfid='.*?'`).FindString(str)
	data["{srcvrfid}"] = regexp.MustCompile(`(srcvrfid='|')`).ReplaceAllString(srcvrfid, ``)
	logfile.Debug("srcvrfid is %s", data["{srcvrfid}"])
	publicip := regexp.MustCompile(`publicip='.*?'`).FindString(str)
	data["{publicip}"] = regexp.MustCompile(`(publicip='|')`).ReplaceAllString(publicip, ``)
	logfile.Debug("publicip is %s", data["{publicip}"])
	publicstartport := regexp.MustCompile(`publicportrange='\d*?~`).FindString(str)
	data["{publicstartport}"] = regexp.MustCompile(`(publicportrange='|~)`).ReplaceAllString(publicstartport, ``)
	logfile.Debug("publicstartport is %s", data["{publicstartport}"])
	publicsendport := regexp.MustCompile(`~\d*?'`).FindString(str)
	data["{publicsendport}"] = regexp.MustCompile(`('|~)`).ReplaceAllString(publicsendport, ``)
	logfile.Debug("publicsendport is %s", data["{publicsendport}"])
	actiontime := regexp.MustCompile(`time='.*?'`).FindString(str)
	data["{actiontime}"] = regexp.MustCompile(`(time='|')`).ReplaceAllString(actiontime, ``)
	logfile.Debug("actiontime is %s", data["{actiontime}"])
	isval := Checknil(data)
	if isval == false {
		logfile.Info("data format error! the value is not enough.")
		return make(map[string]string)
	}
	return data
}

//Checknil check the map val is not nil
func Checknil(m map[string]string) bool {
	for _, v := range useattr {
		if len(m[v]) == 0 {
			logfile.Info("%s is not null!", v)
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
				xmlnode = append(xmlnode, attrname)

			}
		case xml.EndElement:
		default:
		}
	}
	return
}
func FormatSql(sql string) (sqlnode []string) {
	//sql = strings.ToUpper(sql)
	sqlnode = strings.Split(sql, " ")
	return
}
func DecodeSyslog(logstr string, xmlnode []string) (lognode map[string]string, err error) {
	str := strings.Replace(logstr, "  ", " ", -1)
	node := strings.Split(str, " ")
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
func EncodeSysLog(lognode map[string]string, sqlnode []string) (result string) {
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
func ReplaceDot(s string) string {
	source := []byte(s)
	tmp := make([]byte, 1024)
	var tmpindex int
	for index, v := range source {
		if v == '[' {
			//fmt.Println(string(source[:index]))
			tmp = source[:index]
			tmpindex = index
			//fmt.Println(string(tmp))
		} else if v == ']' {
			//fmt.Println(string(source[tmpindex+1 : index]))
			tmp = append(tmp, source[tmpindex+1:index]...)
		}
	}
	return string(tmp)
}
func WriteSysLog() {

	for {
		rawdata := <-datachan
		str := string(rawdata)
		lognode, err := DecodeSyslog(ReplaceDot(str), xmlnode)
		if err != nil {
			logfile.Info("%s|%s", str, err.Error())
			continue
		}
		l4, _ := lognode["L4"]
		if l4 == "6" {
			lognode["L4"] = "TCP"
		} else if l4 == "17" {
			lognode["L4"] = "UDP"
		} else {
			continue
		}

		str = EncodeSysLog(lognode, sqlnode)
		var info SessionInfo
		y, _ := lognode["Year"]
		m, _ := lognode["Mon"]
		d, _ := lognode["Day"]
		h, _ := lognode["Hms"]
		msgid, _ := lognode["MsgId"]
		pubip, _ := lognode["OriSIp"]
		pubport, _ := lognode["TranFPort"]
		key := pubip + pubport
		//info.timestamp = CalcTime(y, m, d, hms)
		month := EncodeMon(m)
		hms := strings.Replace(h, ":", "", -1)
		tstr := y + month + d + hms
		info.atime = tstr
		info.wtime = tstr
		info.info = str
		logfile.Debug("info = %s,tstr = %s ,key = %s, msgid = %d.", info.info, tstr, key, msgid[len(msgid)-1])
		//fmt.Println(info.info, tstr, key, msgid[len(msgid)-1])
		session, ok := GetUserSession(key, info, msgid[len(msgid)-1])
		if ok {
			str = str + session.atime + " " + session.wtime + "\n"
			writedata <- str
		}
	}
}
func WriteToFile() {
	for {
		str := <-writedata
		//fmt.Println(str)
		logfile.Debug("write to file=%s", str)
		if fileindex != tmpindex {
			f.Close()
			filename = fmt.Sprintf("log/%s_%d.log", time.Now().Format("20060102150405"), fileindex)
			f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
			if err != nil {
				panic(err.Error())
			}
			tmpindex = fileindex
		}
		if (time.Now().Unix()-timecount >= int64(filetimespan*60)) || (count >= uint64(filecount)) {
			fileindex++
			timecount = time.Now().Unix()
			count = 0
		}
		if fileindex == 5 {
			fileindex = 0
		}
		count++
		io.WriteString(f, str)
	}

}
func GetUserSession(key string, info SessionInfo, stype byte) (SessionInfo, bool) {
	var usersessioninfo SessionInfo
	var r = false
	mutex.Lock()
	value, ok := userinfo[key]
	if ok {
		switch stype {
		case 'A':
			delete(userinfo, key)
			userinfo[key] = info
		case 'W':
			usersessioninfo.info = info.info
			usersessioninfo.atime = value.atime
			usersessioninfo.wtime = info.wtime
			delete(userinfo, key)
			r = true
		}
	} else {
		switch stype {
		case 'A':
			userinfo[key] = info
		case 'W':
			usersessioninfo = info
			delete(userinfo, key)
			r = true
		}
	}
	mutex.Unlock()
	return usersessioninfo, r
}
func CalcTimeStamp(tstart string, tstop string) string {
	timestart, _ := strconv.Atoi(tstop)
	timestop, _ := strconv.Atoi(tstop)

	return strconv.Itoa((timestart - timestop))
}
func CalcTime(y string, m string, d string, hms string) int64 {
	var timet int64
	timestr := y + " " + m + " " + d + " " + hms
	fmt.Println(timestr)
	t, err := time.Parse("2006 Jan 02 15:04:05", timestr)
	if err != nil {
		timet = 0
	} else {
		timet = t.Unix()
	}
	fmt.Println("time is ", timet)
	return timet
}
func EncodeMon(m string) string {
	var month string
	switch m {
	case "Jan":
		month = "01"
	case "Feb":
		month = "02"
	case "Mar":
		month = "03"
	case "Apr":
		month = "04"
	case "May":
		month = "05"
	case "Jun":
		month = "06"
	case "Jul":
		month = "07"
	case "Aug":
		month = "08"
	case "Sep":
		month = "09"
	case "Oct":
		month = "10"
	case "Nov":
		month = "11"
	case "Dec":
		month = "12"
	}
	return month
}
