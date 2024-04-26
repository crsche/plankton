package main

import (
	"context"
	"crypto/sha512"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto"
	"github.com/go-rod/stealth"
	"github.com/miekg/dns"

	// "github.com/valyala/fasthttp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	// Raw JSON input {category: [url]}
	RawInput map[string][]string
	// Flattened JSON: [{url, category}]
	Input []InputSite
	// Because no tuples :(
	InputSite struct {
		Category string
		Url      url.URL
	}
	Trial struct {
		Time           time.Time
		BrowserVersion proto.BrowserGetVersionResult
		TrialNum       int
		Resources      []Resource
	}
	Resource struct {
		TrialNum   int `bson:"trial_num"`
		Url        url.URL
		Hostname   string
		RespCode   int `bson:"response_code"`
		Hash       [sha512.Size]byte
		Size       int
		DnsAnswers []dns.RR_Header `bson:"dns_answers"`
		Errors     []error
	}
)

// Flatten categories
func (r RawInput) Process() (res Input, e error) {
	for category, sites := range r {
		for _, urlStr := range sites {
			url, e := url.Parse(urlStr)
			if e != nil {
				return nil, e
			}
			res = append(res, InputSite{category, *url})
		}
	}
	return res, nil
}

func GetDNSAnswers(fqdn string, client *dns.Client, conf *dns.ClientConfig) (res []dns.RR_Header, e error) {
	m := dns.Msg{}
	m.SetQuestion(fqdn, dns.TypeA)
	r, _, e := client.Exchange(&m, net.JoinHostPort(conf.Servers[0], conf.Port))
	if e != nil {
		return nil, e
	}
	for _, a := range r.Answer {
		res = append(res, *a.Header())
	}
	return res, nil
}

var (
	input string
	// trialNum     int
	maxTabs      int
	loadTimeout  int
	resRetries   int
	intervalMins int

	dbURI          string
	dbName         string
	collectionName string

	logLevel string
	LOG      *zap.SugaredLogger
)

func main() {
	flag.StringVar(&input, "i", "sites.json", "Path to input file, should be JSON in the specified format")
	// flag.IntVar(&trialNum, "trial", 0, "Trial number")
	// flag.IntVar(&maxBrowsers, "browsers", 1, "Number of concurrent browsers")
	flag.IntVar(&maxTabs, "tabs", 4, "Number of concurrent tabs")
	flag.IntVar(&loadTimeout, "timeout", 15000, "Page load timeout in milliseconds")
	// flag.IntVar(&idleTime, "idle", 1000, "Maximum idle time between network requests in milliseconds")
	flag.StringVar(&dbName, "db", "plankton", "Name of the database to use")
	flag.StringVar(&collectionName, "trials", "sites", "Name of the collection to use")
	flag.StringVar(&dbURI, "db", "mongodb://localhost:27017", "URI of the MongoDB instance")
	flag.StringVar(&logLevel, "ll", "info", "Log level to use")
	flag.IntVar(&resRetries, "rr", 1, "Number of times the browser attempts to retry failed responses")
	flag.IntVar(&intervalMins, "interval", 0, "Interval between trials in minutes")

	flag.Parse()

	//! Init logging
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder // Enable color
	level, e := zapcore.ParseLevel(logLevel)
	if e != nil {
		log.Panicf("failed to parse log level of %s: %v", "debug", e)
	}
	config.Level.SetLevel(level)
	rawLog, e := config.Build()
	if e != nil {
		log.Panicf("failed to build logger config: %v", e)
	}
	LOG = rawLog.Sugar()
	LOG.Info("initialized logger")

	//! Parse input
	var categories RawInput
	f, e := os.Open(input)
	if e != nil {
		LOG.Panicf("failed to open %s as input: %v\n", input, e)
	}
	e = json.NewDecoder(f).Decode(&categories)
	if e != nil {
		LOG.Panicf("failed to parse %s (make sure it's in the right format): %v\n", input, e)
	}
	sites, e := categories.Process()
	if e != nil {
		LOG.Panicf("failed to process input: %v", e)
	}
	LOG.Infof("got input from %s", input)

	//! Init DB
	// Actual init
	dbCtx := context.Background()
	dbClient, e := mongo.Connect(dbCtx, options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if e != nil {
		LOG.Panicf("Failed to connect to DB: %v", e)
	}
	LOG.Infof("Connected to MongoDB with URI %s", "mongodb://127.0.0.1:27017")
	// Verify DB connection
	e = dbClient.Ping(dbCtx, readpref.Primary())
	if e != nil {
		LOG.Panicf("Client couldn't connect to the DB: %v", e)
	}
	client, e := mongo.Connect(context.TODO(), options.Client().ApplyURI(dbURI))
	if e != nil {
		LOG.Panicf("failed to connect to MongoDB: %v", e)
	}
	collection := client.Database(dbName).Collection(collectionName)
	LOG.Infof("Connected to `%s` collection on `%s` DB", dbName, collectionName)

	//! Init DNS client
	dnsConf, e := dns.ClientConfigFromFile("/etc/resolv.conf")
	if e != nil {
		LOG.Panicf("failed to load DNS config from %s: %v", "/etc/resolv.conf", e)
	}
	if len(dnsConf.Servers) < 1 {
		LOG.Panic("DNS conf contained no servers")
	}
	LOG.Infof("using DNS server of %s", dnsConf.Servers[0])
	dnsClient := dns.Client{}

	b := rod.New().MustConnect().MustIncognito().NoDefaultDevice()
	defer b.MustClose()
	version, e := b.Version()
	if e != nil {
		LOG.Panicf("failed to get browser version: %v", e)
	}

	pp := make(chan *rod.Page, maxTabs)
	for i := 0; i < cap(pp); i++ {
		// pp <- b.MustPage()
		pp <- stealth.MustPage(b)
	}
	page := stealth.MustPage(b)
	LOG.Infof("preparing bot report")
	GenerateBotReport(page, "./bot_report.png")
	LOG.Infof("created page pool with %d pages", len(pp))

	// TODO: 1s gap

	//! Data collection
	// LOG.Infof("starting data collection with %d tabs", maxTabs)
	tnum := 0
	for {
		start := time.Now()
		var wg sync.WaitGroup
		for _, site := range sites {
			wg.Add(1)
			go GetRequests(tnum, version, pp, site, &dnsClient, dnsConf, collection, &wg)
		}
		wg.Wait()
		tnum++
		end := time.Now()
		elapsed := end.Sub(start)
		LOG.Infof("trial %d took %s", tnum, elapsed)
		time.Sleep(time.Duration(intervalMins) * time.Minute)
		LOG.Infof("waiting %d minutes", intervalMins)
	}
}

func GenerateBotReport(page *rod.Page, screenshotOut string) {
	page.MustNavigate("https://bot.sannysoft.com")
	page.MustScreenshot(screenshotOut)
}

func GetRequests(tnum int, bv *proto.BrowserGetVersionResult, pp chan *rod.Page, site InputSite, dnsClient *dns.Client, dnsConf *dns.ClientConfig, collection *mongo.Collection, wg *sync.WaitGroup) {
	page := <-pp
	if page == nil {
		LOG.Panicf("%s: failed to get page from pool", site.Url.String())
	}

	// site.Url = "https://" + site.Url
	LOG.Infof("%s: starting request gathering", site.Url.String())

	var resources []Resource
	router := page.HijackRequests().MustAdd("", func(ctx *rod.Hijack) {
		u := ctx.Request.URL()
		LOG.Debugf("%s -> %s", site.Url, u)
		hostname := u.Hostname()

		var errors []error
		var e error
		for i := 0; i < resRetries; i++ {
			e = ctx.LoadResponse(http.DefaultClient, true)
			if e != nil {
				errors = append(errors, e)
				LOG.Infof("%s: failed to load response (try %d) for %s: %v", site.Url.String(), i, hostname, e)
				continue
			} else {
				break
			}
		}
		if e != nil {
			LOG.Warnf("%s: failed to load response for %s after %d tries: %v", site.Url.String(), u, resRetries, e)
		}

		payload := ctx.Response.Payload()
		size := len(payload.Body)
		code := ctx.Response.Payload().ResponseCode
		hash := sha512.Sum512(payload.Body)

		// Get DNS answers - we have to make it a FQDN
		// dnsAnswers, e := GetDNSAnswers(hostname+".", dnsClient, dnsConf)
		// if e != nil {
		// 	errors = append(errors, e)
		// 	LOG.Infof("%s: failed to get DNS answers for %s: %v", site.Url, hostname, e)
		// }
		// TODO: DNS answers
		resources = append(resources, Resource{tnum, *u, hostname, code, hash, size, nil, errors})
	})
	go router.Run()

	e := page.Navigate(site.Url.String())
	if e != nil {
		LOG.Errorf("%s: %v", site.Url, e)
	}
	time.Sleep(time.Duration(loadTimeout * int(time.Millisecond)))
	router.MustStop()
	LOG.Infof("%s: got %d requests", site.Url.String(), len(resources))

	trial := Trial{time.Now(), *bv, tnum, resources}

	// Insert or update the new trial
	True := true
	res, e := collection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: site.Url}, {Key: "category", Value: site.Category}}, bson.D{{Key: "$push", Value: bson.D{{Key: "trials", Value: trial}}}}, &options.UpdateOptions{Upsert: &True})
	if e != nil {
		LOG.Errorf("%s: failed to update trial: %v", site.Url, e)
	}
	if res.UpsertedID != nil {
		LOG.Infof("%s: created new db entry", site.Url.String())
	}
	LOG.Infof("%s: inserted new trial", site.Url.String())

	// page.MustNavigate("about:blank")
	pp <- page
	LOG.Infof("%s: returned page to pool", site.Url.String())
	wg.Done()
}
