package main

import (
	"context"
	"crypto/sha512"
	"encoding/json"
	"flag"
	"log"
	"net"
	u "net/url"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/playwright-community/playwright-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	// "golang.org/x/exp/maps"

	"github.com/alitto/pond"
	"github.com/orcaman/concurrent-map/v2"
	"os"
)

type (
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
		BrowserVersion string
		TrialNum       int
		Resources      []Resource
		// Requests       []Request
		// Responses      []Response
	}
	Resource struct {
		TrialNum int `bson:"trial_num"`
		// Request
		Url          string
		Hostname     string
		ResourceType string          `bson:"resource_type"`
		DnsAnswers   []dns.RR_Header `bson:"dns_answers"`
		// Response
		RespCode int `bson:"response_code"`
		Hash     [sha512.Size]byte
		Size     int
	}

	// Request struct {
	// 	Url          string
	// 	Hostname     string
	// 	ResourceType string          `bson:"resource_type"`
	// 	DnsAnswers   []dns.RR_Header `bson:"dns_answers"`
	// }
	// Response struct {
	// 	RespCode int `bson:"response_code"`
	// 	Hash     [sha512.Size]byte
	// 	Size     int
	// }
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
	input    string
	trialNum int

	maxTabs        int
	loadTimeout    int
	browserVersion string

	dbName         string
	collectionName string

	logLevel string
	LOG      *zap.SugaredLogger
)

func main() {
	flag.StringVar(&input, "i", "sites.json", "Path to input file, should be JSON in the specified format")
	flag.IntVar(&trialNum, "trial", 0, "Trial number")
	flag.IntVar(&maxTabs, "tabs", 8, "Number of concurrent tabs")
	flag.IntVar(&loadTimeout, "timeout", 15000, "Page load timeout in milliseconds")
	flag.StringVar(&dbName, "db", "ephemerals-v3", "Name of the database to use")
	flag.StringVar(&collectionName, "coll", "sites", "Name of the collection to use")
	flag.StringVar(&logLevel, "ll", "info", "Log level to use")

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
	sites := categories.Flatten()
	LOG.Infof("got input from %s", input)

	//! Init DB
	// Actual init
	dbCtx := context.Background()
	dbClient, e := mongo.Connect(dbCtx, options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if e != nil {
		LOG.Panicf("failed to connect to DB: %v", e)
	}
	LOG.Infof("connected to MongoDB with URI %s", "mongodb://127.0.0.1:27017")
	// Verify DB connection
	e = dbClient.Ping(dbCtx, readpref.Primary())
	if e != nil {
		LOG.Panicf("client couldn't connect to the DB: %v", e)
	}
	collection := dbClient.Database(dbName).Collection(collectionName)
	LOG.Infof("connected to `%s` collection on `%s` DB", dbName, collectionName)

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

	pw, e := playwright.Run()
	if e != nil {
		LOG.Panicf("failed to launch Playwright: %v", e)
	}
	LOG.Info("launched Playwright")

	// False := false
	b, e := pw.Chromium.Launch() //playwright.BrowserTypeLaunchOptions{Headless: &False})
	if e != nil {
		LOG.Panicf("could not launch Chromium: %v", e)
	}
	LOG.Info("launched Chromium")
	browserVersion = b.Version()

	pp := make(chan playwright.Page, maxTabs)

	for i := 0; i < maxTabs; i++ {
		p, e := b.NewPage()
		if e != nil {
			LOG.Panicf("failed to create %d page: %v", i, e)
		}
		pp <- p
	}
	LOG.Infof("created page pool with %d pages", len(pp))

	bufferTime := time.Duration((loadTimeout / maxTabs) * int(time.Millisecond))
	LOG.Debugf("buffer time: %fs", bufferTime.Seconds())

	// sitePool := pond.New(maxTabs, )
	var wg sync.WaitGroup
	for i, site := range sites {
		if i < maxTabs {
			time.Sleep(bufferTime)
		}
		wg.Add(1)
		go getSite(pp, site, collection, dnsConf, &dnsClient, &wg)
	}
	wg.Wait()

}

func getSite(pp chan playwright.Page, site InputSite, collection *mongo.Collection, dnsConf *dns.ClientConfig, dnsClient *dns.Client, wg *sync.WaitGroup) {
	page := <-pp
	if page == nil {
		LOG.Panicf("%s: failed to get page from pool", site.Hostname)
	}

	defer func() {
		_, e := page.Goto("about:blank")
		if e != nil {
			LOG.Panicf("%s: failed to reset page: %v", site.Hostname, e)
		}
		pp <- page
		LOG.Infof("%s: returned page to pool", site.Hostname)
		wg.Done()
	}()

	dnsCache := cmap.New[[]dns.RR_Header]()

	LOG.Infof("%s: starting collection", site.Hostname)
	var resources []Resource
	// var m sync.Mutex
	reqPool := pond.New(1000, 0)
	page.On("request", func(req playwright.Request) {
		if !reqPool.Stopped() {
			reqPool.Submit(func() {
				rawUrl := req.URL()
				LOG.Debugf("%s -> %s", site.Hostname, rawUrl)
				rtype := req.ResourceType()

				url, e := u.ParseRequestURI(rawUrl)
				var dnsAnswers []dns.RR_Header
				var hostname string
				if e != nil {
					LOG.Warnf("%s: failed to parse url of %s: %v", site.Hostname, rawUrl, e)
				} else {
					hostname = url.Hostname()
					var cached bool
					dnsAnswers, cached = dnsCache.Get(hostname)
					if cached {
						LOG.Debugf("%s: got %d CACHED DNS answers for %s", site.Hostname, len(dnsAnswers), hostname)
					} else {
						dnsAnswers, e = GetDNSAnswers(dns.Fqdn(hostname), dnsClient, dnsConf)
						if e != nil {
							LOG.Errorf("%s: failed to get DNS answers for %s: %v", site.Hostname, hostname, e)
						} else {
							LOG.Debugf("%s: got %d NEW DNS answers for %s", site.Hostname, len(dnsAnswers), hostname)
							if len(dnsAnswers) > 0 {
								dnsCache.Set(hostname, dnsAnswers)
								LOG.Debugf("%s: cached DNS answers for %s", site.Hostname, hostname)
							}
						}
					}
				}

				resp, e := req.Response()
				if e != nil {
					LOG.Warnf("%s: failed to get repsonse for %s", site.Hostname, rawUrl)
				} else if resp != nil {
					resp.Finished()
					rcode := resp.Status()

					var size int
					body, e := resp.Body()
					if e != nil {
						LOG.Warnf("%s: failed to get body for response from %s: %v", site.Hostname, rawUrl, e)
						size = -1
					} else {
						size = len(body)
					}
					hash := sha512.Sum512(body)

					resource := Resource{TrialNum: trialNum, Url: rawUrl, Hostname: hostname, ResourceType: rtype, DnsAnswers: dnsAnswers, RespCode: rcode, Hash: hash, Size: size}
					// m.Lock()
					resources = append(resources, resource)
					// m.Unlock()
					LOG.Debugf("%s: pushed new resource for %s", site.Hostname, rawUrl)
				}
			})
		}
	})

	start := time.Now()
	loadTime := time.Duration(int(time.Millisecond) * loadTimeout)
	_, e := page.Goto("https://" + site.Hostname)
	if e != nil {
		LOG.Errorf("%s: failed to navigate: %v", site.Hostname, e)
		return
	}
	LOG.Debugf("%s: sleeping for %fs", site.Hostname, loadTime.Seconds())
	time.Sleep(loadTime)
	LOG.Debugf("%s: finished waiting for timeout", site.Hostname)

	reqPool.Stop()
	LOG.Debugf("%s: finished waiting for pools", site.Hostname)

	LOG.Infof("%s: got %d resources", site.Hostname, len(resources))

	trial := Trial{Time: start, BrowserVersion: browserVersion, TrialNum: trialNum, Resources: resources}
	LOG.Debugf("%s: constructed trial", site.Hostname)

	// Insert or update the new trial
	True := true
	res, e := collection.UpdateOne(context.Background(), bson.D{{Key: "url", Value: site.Hostname}, {Key: "category", Value: site.Category}}, bson.D{{Key: "$push", Value: bson.D{{Key: "trials", Value: trial}}}}, &options.UpdateOptions{Upsert: &True})
	if e != nil {
		LOG.Warnf("%s: failed to update trial: %v", site.Hostname, e)
		return
	}
	if res.UpsertedID != nil {
		LOG.Infof("%s: created new db entry", site.Hostname)
	}
	LOG.Infof("%s: inserted new trial", site.Hostname)
}
