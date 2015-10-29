package main

import (
	"fmt"
	"log"
        "time"
        "io"
        "io/ioutil"
        "syscall"
        "strconv"
        "encoding/json"
        "flag"
        "regexp"
        "net"
        "net/http"
	"github.com/streadway/amqp"
        "github.com/abh/geoip"
        "github.com/oschwald/geoip2-golang"
        "github.com/garyburd/redigo/redis"
)

// Json Message received from api v1 GetRules
// json_message := "{ "rules" : [ { "fqdn" : "origin.cdn.afrostream.net", weight: 0.5 }, { "fqdn" : "hw.cdn.afrostream.net", weight: 0.5 } ] }"

var (
  pool *redis.Pool
  redisServer = flag.String("redisServer", ":6379", "")
  redisPassword = flag.String("redisPassword", "", "")
  redisRealSessionTimeout = 86400
  redisSessionTimeout = redisRealSessionTimeout - 90
)

type JsonGetFQDNList struct {
  IP string
}

type Message struct {
  Event_type string
  User_id int
  Fqdn string
  Os string
  Os_version string
  Web_browser string
  Web_browser_version string
  Resolution_size string
  Flash_version string
  Html5_video bool
  Relative_url string
}

type Session struct {
  User_id int
  Start_date time.Time
  ticker *time.Ticker
  ticker_done chan bool
  Fqdn string
  Os string
  Os_version string
  Web_browser string
  Web_browser_version string
  Resolution_size string
  Flash_version string
  Html5_video bool
  Relative_url string
  Ip string
  User_agent string
  Protocol string
  Country string
  City string
  As_number int
  As_name string
  Bandwidth_avg float64
  Bandwidth_current float64
  Bandwidth_date time.Time
  Buffering_number int
}

type ASNStats struct {
  ASName string
  UserCount int
  TotalBandwidth float64
  AverageBandwidth float64
  TotalBufferings int
  AverageBufferings float64
  totalAverageBandwidth float64
}

type ASNHashtable map[int]*ASNStats
var asnDatas ASNHashtable

type CountryStats struct {
  UserCount int
  TotalBandwidth float64
  AverageBandwidth float64
  TotalBufferings int
  AverageBufferings float64
  totalAverageBandwidth float64
}

type CountryHashtable map[string]*CountryStats
var countryDatas CountryHashtable
var originDatas CountryHashtable
var cityDatas CountryHashtable

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s\n", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
  }
}

func logOnError(err error, format string, v ...interface{}) {
  format = format + ": %s"
  if err != nil {
    log.Printf(format, v, err)
  }
}

func newPool(server, password string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 3,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }
            if password != "" {
              if _, err := c.Do("AUTH", password); err != nil {
                  c.Close()
                  return nil, err
              }
            }
            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

func (asnh ASNHashtable) Get(user_id int, create bool) (v *ASNStats, value_exist bool) {
  var ok bool

  value_exist = false
  v, ok = asnh[user_id]
  if ok == false {
    if create == true {
      v = new(ASNStats)
      asnh[user_id] = v
    }
  } else {
    value_exist = true
  }

  return
}

func (ch CountryHashtable) Get(country string, create bool) (v *CountryStats, value_exist bool) {
  var ok bool

  value_exist = false
  v, ok = ch[country]
  if ok == false {
    if create == true {
      v = new(CountryStats)
      ch[country] = v
    }
  } else {
    value_exist = true
  }

  return
}

// Make global stats every seconds
func makeGlobalStats() {
  redisConn := pool.Get()
  defer redisConn.Close()

  for {
    asnDatas = make(map[int]*ASNStats)
    countryDatas = make(map[string]*CountryStats)
    originDatas = make(map[string]*CountryStats)
    cityDatas = make(map[string]*CountryStats)

    vals, err := redis.Strings(redisConn.Do("KEYS", "session.*"))
    log.Printf("RESULT OF KEYS session.* = %v", vals)
    if err == nil {
      for _, v := range vals {
        value, err := redis.Int(redisConn.Do("TTL", v))
        logOnError(err, "Failed to get TTL for a key on Redis")
        log.Printf("rediskey is %s value is %d", v, value)
        if value <= redisSessionTimeout {
          _, err := redisConn.Do("DEL", v)
          logOnError(err, "Failed to DEL a key on Redis")
          continue
        }

        json_s, err := redis.String(redisConn.Do("GET", v))
        logOnError(err, "Failed to GET session on Redis")
        var s Session
        err = json.Unmarshal([]byte(json_s), &s)
        logOnError(err, "Failed to Unmarshal Json on GetUsers")

        log.Printf("[s] is %v", s)

        if s.Bandwidth_current != 0 {
          var as *ASNStats
          as, _ = asnDatas.Get(s.As_number, true)
          as.UserCount += 1
          as.TotalBandwidth += s.Bandwidth_current
          as.totalAverageBandwidth += s.Bandwidth_avg
          as.AverageBandwidth = as.totalAverageBandwidth / float64(as.UserCount)
          as.TotalBufferings += s.Buffering_number
          as.AverageBufferings = float64(as.TotalBufferings) / float64(as.UserCount)
          as.ASName = s.As_name

          var cs *CountryStats
          cs, _ = countryDatas.Get(s.Country, true)
          cs.UserCount++
          cs.TotalBandwidth += s.Bandwidth_current
          cs.totalAverageBandwidth += s.Bandwidth_avg
          cs.AverageBandwidth = cs.totalAverageBandwidth / float64(cs.UserCount)
          cs.TotalBufferings += s.Buffering_number
          cs.AverageBufferings = float64(cs.TotalBufferings) / float64(cs.UserCount)

          var fqdns *CountryStats
          fqdns, _ = originDatas.Get(s.Fqdn, true)
          fqdns.UserCount++
          fqdns.TotalBandwidth += s.Bandwidth_current
          fqdns.totalAverageBandwidth += s.Bandwidth_avg
          fqdns.AverageBandwidth = cs.totalAverageBandwidth / float64(cs.UserCount)
          fqdns.TotalBufferings += s.Buffering_number
          fqdns.AverageBufferings = float64(cs.TotalBufferings) / float64(cs.UserCount)

          var citys *CountryStats
          citys, _ = cityDatas.Get(s.City, true)
          citys.UserCount++
          citys.TotalBandwidth += s.Bandwidth_current
          citys.totalAverageBandwidth += s.Bandwidth_avg
          citys.AverageBandwidth = citys.totalAverageBandwidth / float64(citys.UserCount)
          citys.TotalBufferings += s.Buffering_number
          citys.AverageBufferings = float64(citys.TotalBufferings) / float64(citys.UserCount)
        }
      }
    }

    for k,v := range asnDatas {
      log.Printf("[ ASN STATS %d ] %v", k, v)
      jsonMessage, err := json.Marshal(v)
      logOnError(err, "Failed to serialize struct ASNStats %v", v)
      if err == nil {
        jsonString := string(jsonMessage)
        log.Printf("json string is %s", jsonString)
        key := "stats.asn." + strconv.Itoa(k)
        _, err = redisConn.Do("SET", key, jsonString)
        logOnError(err, "Failed to SET %s with JSON message %s on Redis", key, jsonString)
        _, err = redisConn.Do("EXPIRE", key, "2")
        logOnError(err, "Failed to EXPIRE %s with 2 seconds delay on Redis", key)
      }
    }

    for k,v := range countryDatas {
      log.Printf("[ CTRY STATS %s ] %v", k, v)
      jsonMessage, err := json.Marshal(v)
      logOnError(err, "Failed to serialize struct CountryStats %v", v)
      if err == nil {
        jsonString := string(jsonMessage)
        log.Printf("json string is %s", jsonString)
        key := "stats.country." + k
        _, err = redisConn.Do("SET", key, jsonString)
        logOnError(err, "Failed to SET %s with JSON message %s on Redis", key, jsonString)
        _, err = redisConn.Do("EXPIRE", key, "2")
        logOnError(err, "Failed to EXPIRE %s with 2 seconds delay on Redis", key)
      }
    }
    for k,v := range originDatas {
      log.Printf("[ ORIGIN STATS %s ] %v", k, v)
      jsonMessage, err := json.Marshal(v)
      logOnError(err, "Failed to serialize struct CountryStats %v", v)
      if err == nil {
        jsonString := string(jsonMessage)
        log.Printf("json string is %s", jsonString)
        key := "stats.origin." + k
        _, err = redisConn.Do("SET", key, jsonString)
        logOnError(err, "Failed to SET %s with JSON message %s on Redis", key, jsonString)
        _, err = redisConn.Do("EXPIRE", key, "2")
        logOnError(err, "Failed to EXPIRE %s with 2 seconds delay on Redis", key)
      }
    }
    for k,v := range cityDatas {
      log.Printf("[ CITY STATS %s ] %v", k, v)
      jsonMessage, err := json.Marshal(v)
      logOnError(err, "Failed to serialize struct CountryStats %v", v)
      if err == nil {
        jsonString := string(jsonMessage)
        log.Printf("json string is %s", jsonString)
        key := "stats.city." + k
        _, err = redisConn.Do("SET", key, jsonString)
        logOnError(err, "Failed to SET %s with JSON message %s on Redis", key, jsonString)
        _, err = redisConn.Do("EXPIRE", key, "2")
        logOnError(err, "Failed to EXPIRE %s with 2 seconds delay on Redis", key)
      }
    }

    time.Sleep(time.Second)
  }
}

func decodeJson(msg []byte, db *geoip.GeoIP, db2 *geoip2.Reader) {
  var msg_i interface{}
  err := json.Unmarshal(msg, &msg_i)
  failOnError(err, "Failed to decode JSON message")

  redisConn := pool.Get()
  defer redisConn.Close()

  m := msg_i.(map[string]interface{})
  user_id_number := int(m["user_id"].(float64))
  user_id_string := strconv.Itoa(user_id_number)
  redisKey := "session." + user_id_string
  redisRealSessionTimeoutString := strconv.Itoa(redisRealSessionTimeout)
  log.Printf("user_id = %s", user_id_string)
  switch m["type"] {
    case "start":
      var s Session

      s.Start_date = time.Now()
      if m["fqdn"] != nil {
        s.Fqdn = m["fqdn"].(string)
      }
      if m["os"] != nil {
        s.Os = m["os"].(string)
      }
      if m["os_version"] != nil {
        s.Os_version = m["os_version"].(string)
      }
      if m["web_browser"] != nil {
        s.Web_browser = m["web_browser"].(string)
      }
      if m["web_browser_version"] != nil {
        s.Web_browser_version = m["web_browser_version"].(string)
      }
      if m["solution_size"] != nil {
        s.Resolution_size = m["resolution_size"].(string)
      }
      if m["flash_version"] != nil {
        s.Flash_version = m["flash_version"].(string)
      }
      if m["html5_video"] != nil {
        s.Html5_video = m["html5_video"].(bool)
      }
      if m["relative_url"] != nil {
        s.Relative_url = m["relative_url"].(string)
      }
      if m["ip"] != nil {
        var err error
        s.Ip = m["ip"].(string)
        ip := net.ParseIP(s.Ip)
        record, err := db2.City(ip)
        logOnError(err, "Failed to get GeoIP City informations")
        s.Country = record.Country.IsoCode
        if s.Country == "" {
          s.Country = "None"
        }
        s.City = record.City.Names["fr"]
        if s.City == "" {
          s.City = record.City.Names["en"]
          if s.City == "" {
            s.City = "None"
          }
        }
	log.Printf("record City names %v", record.City.Names)
        name, _ := db.GetName(s.Ip)
        logOnError(err, "Failed to get GeoIP ASN informations")
        asnum_string := name[2:]
        var i int
        for i = 0; asnum_string[i] != ' '; i++ {
        }
        s.As_number, err = strconv.Atoi(asnum_string[:i])
        logOnError(err, "Failed to convert ASNumber to int")
        s.As_name = asnum_string[i+1:]
      }
      if m["user_agent"] != nil {
        s.User_agent = m["user_agent"].(string)
      }
      if m["protocol"] != nil {
        s.Protocol = m["protocol"].(string)
      }
      if m["video_bitrate"] != nil && m["audio_bitrate"] != nil {
        s.Bandwidth_current = m["video_bitrate"].(float64) + m["audio_bitrate"].(float64)
      } else {
        s.Bandwidth_current = 0
      }
      s.Bandwidth_avg = s.Bandwidth_current
      s.Bandwidth_date = s.Start_date
      s.Buffering_number = 0
      s.User_id = user_id_number
      serialized, err := json.Marshal(s)
      log.Printf("serialized is %s", string(serialized))
      logOnError(err, "Cannot serialize session with session %v", s)
      if err == nil {
        _, err = redisConn.Do("SET", redisKey, string(serialized))
        _, err = redisConn.Do("EXPIRE", redisKey, redisRealSessionTimeoutString)
        logOnError(err, "Failed to set user_id %s with JSON message %s on Redis", user_id_string, serialized)
      }
    case "bandwidthDecrease":
      fallthrough
    case "bandwidthIncrease":
      json_s, err := redis.String(redisConn.Do("GET", user_id_string))
      if err != nil {
        log.Printf("key doesn't exist in Redis")
        log.Printf("err = %v, json_s = %v", err, json_s)
      } else {
        var s Session
        err := json.Unmarshal([]byte(json_s), &s)

        t := time.Now()
        if s.Bandwidth_avg == 0 {
          s.Bandwidth_avg = m["video_bitrate"].(float64) + m["audio_bitrate"].(float64)
        } else {
          bw_time := float64(s.Bandwidth_date.Unix() - s.Start_date.Unix())
          bw_current_time := float64(t.Unix() - s.Bandwidth_date.Unix())
          total_time := float64(t.Unix() - s.Start_date.Unix())
          s.Bandwidth_avg = ((s.Bandwidth_avg * bw_time) + (s.Bandwidth_current * bw_current_time)) / total_time
        }
        s.Bandwidth_current = m["video_bitrate"].(float64) + m["audio_bitrate"].(float64)
        s.Bandwidth_date = t
        serialized, err := json.Marshal(s)
        log.Printf("struct session is %v", s)
        log.Printf("serialized is %s", string(serialized))
        logOnError(err, "Cannot serialize session with session %v", s)
        if err == nil {
          _, err = redisConn.Do("SET", redisKey, string(serialized))
          _, err = redisConn.Do("EXPIRE", redisKey, redisRealSessionTimeoutString)
          logOnError(err, "Failed to set user_id %s with JSON message %s on Redis", user_id_string, serialized)
        }
      }
    case "buffering":
      json_s, err := redis.String(redisConn.Do("GET", redisKey))
      if err != nil {
        log.Printf("key doesn't exist in Redis")
      } else {
        var s Session
        err := json.Unmarshal([]byte(json_s), &s)

        s.Buffering_number++

        serialized, err := json.Marshal(s)
        log.Printf("serialized is %s", string(serialized))
        logOnError(err, "Cannot serialize session with session %v", s)
        if err == nil {
          _, err = redisConn.Do("SET", redisKey, string(serialized))
          _, err = redisConn.Do("EXPIRE", redisKey, redisRealSessionTimeoutString)
          logOnError(err, "Failed to set user_id on Redis")
        }
      }
    case "ping":
      status, err := redisConn.Do("EXPIRE", redisKey, redisRealSessionTimeoutString)
      log.Printf("err = %v, status = %v",  err, status)
  }
}

func httpServerGetUsers(w http.ResponseWriter, r *http.Request) {
  redisConn := pool.Get()
  defer redisConn.Close()

  data := false
  vals, err := redis.Values(redisConn.Do("KEYS", "session.*"))
  logOnError(err, "Failed to get keys from Redis")
  for _, v := range vals {
    json_s, err := redis.String(redisConn.Do("GET", v))
    logOnError(err, "Failed to GET session on Redis")
    var s Session
    err = json.Unmarshal([]byte(json_s), &s)
    logOnError(err, "Failed to Unmarshal Json on GetUsers")

    timeout, err := redis.Int(redisConn.Do("TTL", v))
    logOnError(err, "Failed to GET TTL for session on Redis")

    data = true
    var bandwidth_current string
    var bandwidth_avg string

    if (s.Bandwidth_current == 0) {
      bandwidth_current =  "N/A"
    } else {
      bandwidth_current = strconv.FormatFloat(s.Bandwidth_current, 'f', 2, 64)
    }
    if (s.Bandwidth_avg == 0) {
      bandwidth_avg = "N/A"
    } else {
      bandwidth_avg = strconv.FormatFloat(s.Bandwidth_avg, 'f', 2, 64)
    }

    fmt.Fprintf(w, `<div style="float:left; width:15vw;">%s</div><div style="float:left; width:5vw;">%d</div><div style="float:left; width:5vw;">%s</div><div style="float:left; width:5vw;">%.12s</div><div style="float:left; width:12vw;">%s</div><div style="float:left; width:7vw;">%d</div><div style="float:left; width:10vw">%.28s</div><div style="float:left; width:12vw;">%s</div><div style="float:left; width:12vw;">%s</div><div style="float:left; width:5vw;">%d</div><div style="float:left; width:8vw;">%d s</div>`, s.Fqdn, s.User_id, s.Country, s.City, s.Ip, s.As_number, s.As_name, bandwidth_avg, bandwidth_current, s.Buffering_number, timeout - redisSessionTimeout)
  }
  if data == false {
    io.WriteString(w, "<br /><h3>No data available for the moment...</h3>")
  }
}

func httpServerGetASNStats(w http.ResponseWriter, r *http.Request) {
  redisConn := pool.Get()
  defer redisConn.Close()

  vals, err := redis.Values(redisConn.Do("KEYS", "stats.asn.*"))
  logOnError(err, "Failed to get KEYS for stats.asn.*")
  if err == nil && len(vals) > 0 {
    for _, v := range vals {
      value, err := redis.String(redisConn.Do("GET", v))
      logOnError(err, "Failed to GET %v from Redis server", v)
      var as *ASNStats
      err = json.Unmarshal([]byte(value), &as)
      logOnError(err, "Failed to Unmarshal JSON message %s", value)
      if err == nil {
        re, err := regexp.Compile("stats.asn.(.*)$")
        logOnError(err, "Failed to compile regexp stats.asn.[.*]")
        if err == nil {
          key := string(v.([]byte))
          log.Printf("V. STRING is %s", key)
          asnum := re.FindStringSubmatch(key)
          fmt.Fprintf(w, `<div style="float:left; width:7vw;">%s</div><div style="float:left; width:10vw;">%.28s</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%.2f</div>`, asnum[1], as.ASName, as.UserCount, as.TotalBandwidth, as.AverageBandwidth, as.TotalBufferings, as.AverageBufferings)
        }
      }
    }
  } else {
    fmt.Fprintf(w, "<br /><h3>No data available for the moment...</h3>")
  }
}

func httpServerGetCountryStats(w http.ResponseWriter, r *http.Request) {
  dataLength := len(countryDatas)
  if dataLength > 0 {
    for k, v := range countryDatas {
      fmt.Fprintf(w, `<div style="float:left; width:7vw;">%s</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%.2f</div>`, k, v.UserCount, v.TotalBandwidth, v.AverageBandwidth, v.TotalBufferings, v.AverageBufferings)
    }
  } else {
    fmt.Fprintf(w, "<br /><h3>No data available for the moment...</h3>")
  }
}

func httpServerGetOriginStats(w http.ResponseWriter, r *http.Request) {
  dataLength := len(originDatas)
  if dataLength > 0 {
    for k, v := range originDatas {
      fmt.Fprintf(w, `<div style="float:left; width:7vw;">%s</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%.2f</div>`, k, v.UserCount, v.TotalBandwidth, v.AverageBandwidth, v.TotalBufferings, v.AverageBufferings)
    }
  } else {
    fmt.Fprintf(w, "<br /><h3>No data available for the moment...</h3>")
  }
}

func httpServerGetCityStats(w http.ResponseWriter, r *http.Request) {
  dataLength := len(cityDatas)
  if dataLength > 0 {
    for k, v := range cityDatas {
      fmt.Fprintf(w, `<div style="float:left; width:7vw;">%s</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%f</div><div style="float:left; width:15vw;">%d</div><div style="float:left; width:15vw;">%.2f</div>`, k, v.UserCount, v.TotalBandwidth, v.AverageBandwidth, v.TotalBufferings, v.AverageBufferings)
    }
  } else {
    fmt.Fprintf(w, "<br /><h3>No data available for the moment...</h3>")
  }
}

func httpServerGetUsersCount(w http.ResponseWriter, r *http.Request) {
  redisConn := pool.Get()
  defer redisConn.Close()

  val, err := redis.Int(redisConn.Do("eval", "return #redis.call('keys', 'session.*')", "0"))
  failOnError(err, "Failed to get INFO from Redis")
  //re := regexp.MustCompile(`db0:keys=([0-9]*),`)
  //key_count := re.FindStringSubmatch(val)
  //if key_count == nil {
  // fmt.Fprintf(w, "0")
  //} else {
  //  fmt.Fprintf(w, "%s", key_count[1])
  //}
  fmt.Fprintf(w, "%d", val)
}

func httpServerGetFQDNList(w http.ResponseWriter, r *http.Request) {
  decoder := json.NewDecoder(r.Body)
  var t JsonGetFQDNList
  err := decoder.Decode(&t)
  logOnError(err, "Failed to get IP address during getFQDNList")
  log.Println(t.IP)
}

func httpServerLoadPage(path string) (content []byte, err error) {
  content, err = ioutil.ReadFile(path)
  logOnError(err, "Cannot read file")

  return
}

func httpServerDefaultRoot(w http.ResponseWriter, r *http.Request) {
  path := r.URL.Path[:]
  html, err := httpServerLoadPage(path)
  if err != nil {
    w.WriteHeader(http.StatusNotFound)
    w.Write([]byte("Page not found"))
  } else {
    io.WriteString(w, string(html))
  }
}

func main() {
  // Init
  base_path := "/home/ubuntu/Go/www"

  db, err := geoip.Open("./GeoIPASNum.dat")
  failOnError(err, "Failed to open GeoIP ASN database")
  
  db2, err := geoip2.Open("./GeoLite2-City.mmdb")
  failOnError(err, "Failed to open GeoIP City database")
  defer db2.Close()

  conn, err := amqp.Dial("amqp://127.0.0.1/")
  failOnError(err, "Failed to connect to RabbitMQ")
  defer conn.Close()

  flag.Parse()
  pool = newPool(*redisServer, *redisPassword)

  err = syscall.Chroot(base_path)
  failOnError(err, "Failed to chroot")

  redisConn := pool.Get()
  defer conn.Close()
  status, err := redisConn.Do("PING")
  fmt.Println(status, err)

  ch, err := conn.Channel()
  failOnError(err, "Failed to open a channel")
  defer ch.Close()

  err = ch.ExchangeDeclare(
    "logs",   // name
    "fanout", // type
    true,     // durable
    false,    // auto-deleted
    false,    // internal
    false,    // no-wait
    nil,      // arguments
  )
  failOnError(err, "Failed to declare an exchange")

  q, err := ch.QueueDeclare(
    "",
    false,
    false,
    true,
    false,
    nil,
  )
  failOnError(err, "Failed to declare a queue")

  err = ch.QueueBind(
    q.Name, // queue name
    "",     // routing key
    "afrostream-api-stats", // exchange
    false,
    nil,
  )
  failOnError(err, "Failed to bind a queue")

  msgs, err := ch.Consume(
    q.Name,
    "",
    true,
    false,
    false,
    false,
    nil,
  )
  failOnError(err, "Failed to register a consumer")

  //forever := make(chan bool)

  go func() {
    for d := range msgs {
      log.Printf("Received a message: %s", d.Body)
      decodeJson(d.Body, db, db2)
    }
  }()

  log.Printf(" [*] Waiting for messages, To exit press CTRL+C")
  //go func() {
  //  <-forever
  //}()

  go makeGlobalStats()

  http.HandleFunc("/", httpServerDefaultRoot)
  http.HandleFunc("/getUsers", httpServerGetUsers)
  http.HandleFunc("/getUsersCount", httpServerGetUsersCount)
  http.HandleFunc("/getFQDNList", httpServerGetFQDNList)
  http.HandleFunc("/getASNStats", httpServerGetASNStats)
  http.HandleFunc("/getCountryStats", httpServerGetCountryStats)
  http.HandleFunc("/getOriginStats", httpServerGetOriginStats)
  http.HandleFunc("/getCityStats", httpServerGetCityStats)
  http.ListenAndServe(":8080", nil)
}
