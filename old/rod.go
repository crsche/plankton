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
	//	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
	"github.com/go-rod/stealth"
	"github.com/miekg/dns"

	// "github.com/valyala/fasthttp"
	cmap "github.com/orcaman/concurrent-map/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	// "github.com/BurntSushi/toml"
)

type (
	Config struct {
		Input           string
		Trial           int
		Tabs            int
		LoadTime        time.Duration
		LogLevel        string
		ResourceRetries int
		Db              struct {
			Uri        string
			Name       string
			Collection string
		}
	}
	// Raw JSON input {category: [url]}
	RawInput map[string][]string
	// Flattened JSON: [{url, category}]
	Input []InputSite
	// Because no tuples :(
	InputSite struct {
		Category string
		Hostname string
	}
	Trial struct {
		Time           time.Time
		BrowserVersion proto.BrowserGetVersionResult
		TrialNum       int
		Resources      []Resource
	}
	Resource struct {
		TrialNum     int `bson:"trial_num"`
		Url          url.URL
		Hostname     string
		ResourceType proto.NetworkResourceType
		RespCode     int `bson:"response_code"`
		Hash         [sha512.Size]byte
		Size         int
		Dns          *dns.Msg
	}
)

// Flatten categories
func (r RawInput) Flatten() (res Input) {
	for k, s := range r {
		for _, v := range s {
			res = append(res, InputSite{k, v})
		}
	}
	return res
}

func GetDNSAnswers(fqdn string, client *dns.Client, conf *dns.ClientConfig) (res *dns.Msg, e error) {
	m := dns.Msg{}
	m.SetQuestion(fqdn, dns.TypeA)
	r, _, e := client.Exchange(&m, net.JoinHostPort(conf.Servers[0], conf.Port))
	return r, e
}

var (
	input      string
	trialNum   int
	maxTabs    int
	loadTime   int
	resRetries int

	dbName         string
	collectionName string

	logLevel string
	LOG      *zap.SugaredLogger
)

func main() {
	flag.StringVar(&input, "i", "sites.json", "Path to input file, should be JSON in the specified format")
	flag.IntVar(&trialNum, "trial", 0, "Trial number")
	flag.IntVar(&maxTabs, "tabs", 32, "Number of concurrent tabs")
	flag.IntVar(&loadTime, "timeout", 15000, "Page load timeout in milliseconds")
	flag.StringVar(&dbName, "db", "ephemerals-v4", "Name of the database to use")
	flag.StringVar(&collectionName, "coll", "sites", "Name of the collection to use")
	flag.StringVar(&logLevel, "ll", "info", "Log level to use")
	flag.IntVar(&resRetries, "rr", 1, "Number of times the browser attempts to retry failed responses")

	flag.Parse()
	// time.ParseDuration()

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
	sites := categories.Flatten()
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
	collection := dbClient.Database(dbName).Collection(collectionName)
	LOG.Infof("Connected to `%s` collection on `%s` DB", dbName, collectionName)

	//! Init DNS client
	dnsConf, e := dns.ClientConfigFromFile("/etc/resolv.conf")
	// dns.ClientConfig
	if e != nil {
		LOG.Panicf("failed to load DNS config from %s: %v", "/etc/resolv.conf", e)
	}
	if len(dnsConf.Servers) < 1 {
		LOG.Panic("DNS conf contained no servers")
	}
	LOG.Infof("using DNS server of %s", dnsConf.Servers[0])
	dnsClient := dns.Client{}

	//u, e := launcher.New().Launch()
	//if e != nil {
	//	LOG.Panicf("failed to launch browser: %v", e)
	//}
	b := rod.New().MustConnect().MustIncognito().NoDefaultDevice() //.ControlURL(u).MustConnect().NoDefaultDevice().MustIncognito()
	version, e := b.Version()
	if e != nil {
		LOG.Panicf("failed to get browser version: %v", e)
	}

	pp := make(chan *rod.Page, maxTabs)
	for i := 0; i < cap(pp); i++ {
		p, e := stealth.Page(b)
		if e != nil {
			LOG.Panicf("failed to create page %d: %v", i, e)
		}
		pp <- p
	}
	LOG.Infof("created page pool with %d pages", len(pp))

	bufferTime := time.Duration((loadTime / maxTabs) * int(time.Millisecond))
	//! Data collection
	var wg sync.WaitGroup
	for i, site := range sites {
		if i > 0 && i < maxTabs {
			time.Sleep(bufferTime)
		}
		wg.Add(1)
		go GetRequests(version, pp, site, &dnsClient, dnsConf, collection, &wg)
	}
	wg.Wait()
}

func GetRequests(bv *proto.BrowserGetVersionResult, pp chan *rod.Page, site InputSite, dnsClient *dns.Client, dnsConf *dns.ClientConfig, collection *mongo.Collection, wg *sync.WaitGroup) {
	page := <-pp
	if page == nil {
		LOG.Panicf("%s: failed to get page from pool", site.Hostname)
	}
	LOG.Infof("%s: starting request gathering", site.Hostname)

	dnsCache := cmap.New[*dns.Msg]()

	var resources []Resource
	router := page.HijackRequests()
	e := router.Add("", "", func(ctx *rod.Hijack) {
		u := ctx.Request.URL()
		rtype := ctx.Request.Type()
		LOG.Debugf("%s -> %s", site.Hostname, u)
		if u == nil {
			LOG.Errorf("%s: request missing URL")
		} else {
			hostname := u.Hostname()
			dnsInfo, cached := dnsCache.Get(hostname)
			if cached {
				LOG.Debugf("%s: got CACHED DNS info for %s", site.Hostname, hostname)
			} else {
				var e error
				dnsInfo, e = GetDNSAnswers(dns.Fqdn(hostname), dnsClient, dnsConf)
				if e != nil {
					LOG.Warnf("%s: failed to get DNS answers for %s: %v", site.Hostname, hostname, e)
				} else if dnsInfo == nil {
					LOG.Warnf("%s: ptr to DNS info for %s was nil", site.Hostname, hostname, e)
				} else if len(dnsInfo.Answer) == 0 {
					LOG.Warnf("%s: got 0 DNS answers for %s", site.Hostname, hostname)
				} else {
					LOG.Debugf("%s: got %d DNS answers for %s", site.Hostname, len(dnsInfo.Answer), hostname)
					dnsCache.Set(hostname, dnsInfo)
				}
			}

			var e error
			for i := 0; i < resRetries; i++ {
				e = ctx.LoadResponse(http.DefaultClient, true)
				if e != nil {
					// TODO: DELAY
					LOG.Infof("%s: failed to load response (try %d) for %s: %v", site.Hostname, i+1, hostname, e)
					continue
				} else {
					break
				}
			}
			if e != nil {
				LOG.Warnf("%s: failed to load response for %s after %d tries: %v", site.Hostname, u, resRetries, e)
			}

			payload := ctx.Response.Payload()
			size := len(payload.Body)
			code := ctx.Response.Payload().ResponseCode
			hash := sha512.Sum512(payload.Body)

			resources = append(resources, Resource{trialNum, *u, hostname, rtype, code, hash, size, dnsInfo})
		}
	})
	go router.Run()

	start := time.Now()
	load := make(chan struct {
		proto.PageNavigateResult
		error
	})
	go func() {
		s, e := proto.PageNavigate{URL: "http://" + site.Hostname}.Call(page)
		load <- struct {
			proto.PageNavigateResult
			error
		}{*s, e}
	}()

	loadTimeout := 10 * time.Second
	waitTime := time.Duration(loadTime * int(time.Millisecond))

	select {
	case res := <-load:
		if res.error != nil {
			LOG.Errorf("%s: failed to navigate: %v", site.Hostname, res.error)
		} else if res.ErrorText != "" {
			LOG.Errorf("%s: failed to navigate: %v", site.Hostname, res.ErrorText)
		} else {
			LOG.Debugf("%s: successfully navigated")
		}
	case <-time.After(loadTimeout):
		LOG.Warnf("%s: exceeded timeout of %fs", loadTimeout.Seconds())
	}
	LOG.Debugf("%s: waiting %fs", waitTime.Seconds())
	time.Sleep(waitTime)
	e = page.StopLoading()
	if e != nil {
		LOG.Errorf("%s: failed to stop loading page: %v", site.Hostname, e)
	}
	e = router.Stop()
	if e != nil {
		LOG.Errorf("%s: failed to stop router: %v", site.Hostname, e)
	}
	LOG.Infof("%s: got %d resources", site.Hostname, len(resources))

	trial := Trial{start, *bv, trialNum, resources}

	// Insert or update the new trial
	True := true
	res, e := collection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: site.Hostname}, {Key: "category", Value: site.Category}}, bson.D{{Key: "$push", Value: bson.D{{Key: "trials", Value: trial}}}}, &options.UpdateOptions{Upsert: &True})
	if e != nil {
		LOG.Errorf("%s: failed to update trial: %v", site.Hostname, e)
	}
	if res.UpsertedID != nil {
		LOG.Infof("%s: created new db entry", site.Hostname)
	}
	LOG.Infof("%s: inserted new trial", site.Hostname)

	pp <- page
	LOG.Infof("%s: returned page to pool", site.Hostname)
	wg.Done()
}
