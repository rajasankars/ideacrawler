/*************************************************************************
 *
 * Copyright 2018 Ideas2IT Technology Services Private Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***********************************************************************/

package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/fetchbot"
	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"github.com/antchfx/xpath"
	"github.com/antchfx/xquery/html"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
	"github.com/ideas2it/ideacrawler/chromeclient"
	"github.com/ideas2it/ideacrawler/prefetchurl"
	pb "github.com/ideas2it/ideacrawler/protofiles"
	"github.com/shsms-i2i/sflag"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

// CUSTOM httpStatusCodes

const (
	HTTPSTATUS_LOGIN_SUCCESS      = 1500
	HTTPSTATUS_LOGIN_FAILED       = 1501
	HTTPSTATUS_NOLONGER_LOGGED_IN = 1502
	HTTPSTATUS_FETCHBOT_ERROR     = 1520
	HTTPSTATUS_RESPONSE_ERROR     = 1530
	HTTPSTATUS_SUBSCRIPTION       = 1540
	HTTPSTATUS_FOLLOW_PARSE_ERROR = 1550
)

var cliParams = struct {
	DialAddress    string "Interface to dial from.  Defaults to OS defined routes|"
	SaveLoginPages string "Saves login pages in this path. Don't save them if empty."
	ListenAddress  string "Interface to listen on. Defaults to 127.0.0.1|127.0.0.1"
	ListenPort     string "Port to listen on. Defaults to 2345|2345"
	LogPath        string "Logpath to log into. Default is stdout."
}{}

type Subscriber struct {
	doneSeqnum int32
	reqChan    chan pb.PageRequest
	sendChan   chan pb.PageHTML
	stopChan   chan bool
	connected  bool
}

type Job struct {
	domainname        string
	opts              *pb.DomainOpt
	sub               pb.Subscription
	prevRun           time.Time
	nextRun           time.Time
	frequency         time.Duration
	runNumber         int32
	running           bool
	done              bool
	seqnum            int32
	callbackUrlRegexp *regexp.Regexp
	followUrlRegexp   *regexp.Regexp
	subscriber        Subscriber
	mu                sync.Mutex
	duplicates        map[string]bool
	cancelChan        chan cancelSignal

	// there will be a goroutine started inside RunJob that will listen on registerDoneListener for
	// DoneListeners.  There will be a despatcher goroutine that will forward the doneChan info to
	// all doneListeners.
	// TODO: Needs to be replaced with close(chan) signals.
	doneListeners        []chan jobDoneSignal
	registerDoneListener chan chan jobDoneSignal
	doneChan             chan jobDoneSignal
	randChan             <-chan int // for random number between min and max norm distributed

	log *log.Logger
}

type cancelSignal struct{}
type jobDoneSignal struct{}

type ideaCrawlerServer struct {
	jobs       map[string]*Job
	newJobChan chan<- NewJob
	newSubChan chan<- NewSub
}

type NewJobStatus struct {
	sub                  pb.Subscription
	subscriber           Subscriber
	cancelChan           chan cancelSignal
	registerDoneListener chan chan jobDoneSignal
	err                  error
}

type NewJob struct {
	opts      *pb.DomainOpt
	retChan   chan<- NewJobStatus
	subscribe bool
}

type NewSub struct {
	sub     pb.Subscription
	retChan chan<- NewJobStatus
}

type CrawlCommand struct {
	method     string
	url        *url.URL
	noCallback bool
	metaStr    string
	urlDepth   int32
}

type IdeaCrawlDoer struct {
	doer fetchbot.Doer
	job  *Job
	sema *semaphore.Weighted
	s    *ideaCrawlerServer
}

func randomGenerator(min int, max int, randChan chan<- int) {
	ii := 0
	jj := 5
	genRand := func(min int, max int) int {
		v := 0
		for v < min {
			v = int((rand.NormFloat64()+1.0)*float64(max-min)/2.0 + float64(min))
		}
		return v
	}
	for ; ; ii++ {
		if ii >= jj {
			jj = genRand(5, 20)
			ii = 0
			randChan <- genRand(max, max*3)
			continue
		}
		randChan <- genRand(min, max)
	}
}

func execPrefetchReq(gc *IdeaCrawlDoer, prefetchReq *http.Request) {
	res, _ := gc.doer.Do(prefetchReq)
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
}

func processPrefetchCSSURL(gc *IdeaCrawlDoer, prefetchCSSReq *http.Request, prefetchCSSURL string) error {
	var err error
	prefetchCSSRes, err := gc.doer.Do(prefetchCSSReq)
	if err != nil {
		log.Printf("ERR - Unable to process prefetch css resource url. Error - %v\n", err)
		return err
	}
	prefetchCSSResBody, err := ioutil.ReadAll(prefetchCSSRes.Body)
	if err != nil {
		log.Printf("ERR - Unable to read css url response content. Error - %v\n", err)
		return err
	}
	prefetchLinks, err := prefetchurl.GetPrefetchURLs(prefetchCSSResBody, prefetchCSSURL)
	if err != nil {
		log.Printf("ERR - While trying to retreive resources urls. Error - %v\n", err)
		return err
	}
	for _, prefetchLink := range prefetchLinks {
		prefetchLinkReq, err := http.NewRequest("GET", prefetchLink, nil)
		if err != nil {
			log.Printf("ERR - Unable to create new request for prefetch urls. Error - %v\n", err)
			return err
		}
		prefetchLinkReq.Header.Set("User-Agent", gc.job.opts.Useragent)
		go execPrefetchReq(gc, prefetchLinkReq)
	}
	return err
}

func processPrefetchURLs(gc *IdeaCrawlDoer, respBody []byte, reqURL string) error {
	var err error
	prefetchLinks, err := prefetchurl.GetPrefetchURLs(respBody, reqURL)
	if err != nil {
		log.Printf("ERR - While trying to retreive resources urls. Error - %v\n", err)
		return err
	}
	for _, prefetchLink := range prefetchLinks {
		prefetchLinkReq, err := http.NewRequest("GET", prefetchLink, nil)
		if err != nil {
			log.Printf("ERR - Unable to create new request for prefetch urls. Error - %v\n", err)
			return err
		}
		prefetchLinkReq.Header.Set("User-Agent", gc.job.opts.Useragent)
		if strings.HasSuffix(prefetchLink, ".css") {
			go processPrefetchCSSURL(gc, prefetchLinkReq, prefetchLink)
		} else {
			go execPrefetchReq(gc, prefetchLinkReq)
		}
	}
	return err
}

func (gc *IdeaCrawlDoer) Do(req *http.Request) (*http.Response, error) {
	opts := gc.job.opts
	if gc.sema == nil {
		gc.sema = semaphore.NewWeighted(int64(opts.MaxConcurrentRequests))
	}
	gc.sema.Acquire(context.Background(), 1)
	defer gc.sema.Release(1)
	if opts.NetworkIface != "" {
		ifs, err := net.Interfaces()
		if err != nil {
			gc.job.log.Printf("ERR - Unable to get list of network interfaces. Error - %v\n", err)
			return nil, err
		}
		ifmatch := false
		for _, iface := range ifs {
			if iface.Name == opts.NetworkIface {
				ifmatch = true
				break
			}
		}
		if ifmatch == false {
			emsg := "Interface '" + opts.NetworkIface + "' not active."
			gc.job.cancelChan <- cancelSignal{}
			gc.job.log.Printf("ERR - %v\n", emsg)
			return nil, errors.New(emsg)
		}
	}
	// randomized delay
	afterTime := opts.MinDelay
	if afterTime < 5 {
		afterTime = 5
	}
	if opts.MaxDelay > afterTime {
		afterTime = int32(<-gc.job.randChan)
		gc.job.log.Printf("Next delay - %v\n", time.Duration(afterTime)*time.Second)
	}
	after := time.After(time.Duration(afterTime) * time.Second)
	resp, err := gc.doer.Do(req)
	// process prefetch urls
	if opts.Chrome == false && opts.Prefetch == true {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("ERR - Unable to read url response content. Error - %v\n", err)
			return nil, err
		}
		err = processPrefetchURLs(gc, respBody, resp.Request.URL.String())
		if err != nil {
			return nil, err
		}
		resp.Body = ioutil.NopCloser(bytes.NewBuffer(respBody))
	}
	<-after
	return resp, err
}

func CreateCommand(method, urlstr, metaStr string, urlDepth int32) (CrawlCommand, error) {
	parsed, err := url.Parse(urlstr)
	return CrawlCommand{
		method:  method,
		url:     parsed,
		metaStr: metaStr,
		urlDepth: urlDepth,
	}, err
}

func (c CrawlCommand) URL() *url.URL {
	return c.url
}

func (c CrawlCommand) Method() string {
	return c.method
}

func (c CrawlCommand) MetaStr() string {
	return c.metaStr
}

func (c CrawlCommand) URLDepth() int32 {
	return c.urlDepth
}

func DomainNameFromURL(_url string) (string, error) { //returns domain name and error if any
	u, err := url.Parse(_url)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}

func (s *ideaCrawlerServer) JobManager(newJobChan <-chan NewJob, newSubChan <-chan NewSub) {
	for {
		select {
		case nj := <-newJobChan:
			log.Println("Received new job", nj.opts.SeedUrl)
			domainname, err := DomainNameFromURL(nj.opts.SeedUrl)
			if err != nil {
				nj.retChan <- NewJobStatus{pb.Subscription{}, Subscriber{}, nil, nil, err}
				continue
			}
			emptyTS, _ := ptypes.TimestampProto(time.Time{})
			sub := pb.Subscription{uuid.New().String(), domainname, pb.SubType_SEQNUM, 0, emptyTS}

			freq, err := ptypes.Duration(nj.opts.Frequency)
			if nj.opts.Repeat == true && err != nil {
				nj.retChan <- NewJobStatus{pb.Subscription{}, Subscriber{}, nil, nil, errors.New("Bad value for DomainOpt.Frequency field - " + domainname + " - " + err.Error())}
				continue
			}
			subr := Subscriber{}
			if nj.subscribe == true {
				subr = Subscriber{0, make(chan pb.PageRequest, 1000), make(chan pb.PageHTML, 1000), make(chan bool, 3), true}
			}
			var callbackUrlRegexp, followUrlRegexp *regexp.Regexp
			if len(nj.opts.CallbackUrlRegexp) > 0 {
				callbackUrlRegexp, err = regexp.Compile(nj.opts.CallbackUrlRegexp)
				if err != nil {
					nj.retChan <- NewJobStatus{pb.Subscription{}, Subscriber{}, nil, nil, errors.New("CallbackUrlRegexp doesn't compile - '" + nj.opts.CallbackUrlRegexp + "' - " + err.Error())}
					continue
				}
			}
			if len(nj.opts.FollowUrlRegexp) > 0 {
				followUrlRegexp, err = regexp.Compile(nj.opts.FollowUrlRegexp)
				if err != nil {
					nj.retChan <- NewJobStatus{pb.Subscription{}, Subscriber{}, nil, nil, errors.New("FollowUrlRegexp doesn't compile - '" + nj.opts.FollowUrlRegexp + "' - " + err.Error())}
					continue
				}
			}
			canc := make(chan cancelSignal)
			regDoneC := make(chan chan jobDoneSignal)
			randChan := make(chan int, 5)
			s.jobs[sub.Subcode] = &Job{domainname, nj.opts, sub, time.Time{}, time.Time{}, freq, 0, false, false, 0, callbackUrlRegexp, followUrlRegexp, subr, sync.Mutex{}, map[string]bool{}, canc, []chan jobDoneSignal{}, regDoneC, make(chan jobDoneSignal), randChan, nil}
			go randomGenerator(int(nj.opts.MinDelay), int(nj.opts.MaxDelay), randChan)
			nj.retChan <- NewJobStatus{sub, subr, canc, regDoneC, nil}
		case ns := <-newSubChan:
			job := s.jobs[ns.sub.Subcode]
			if job == nil {
				ns.retChan <- NewJobStatus{pb.Subscription{}, Subscriber{}, nil, nil, errors.New("Unable to find subcode - " + ns.sub.Subcode)}
				continue
			}
			ns.retChan <- NewJobStatus{job.sub, job.subscriber, job.cancelChan, job.registerDoneListener, nil}
		default:
			time.Sleep(50 * time.Millisecond)
		}
		for domainname, job := range s.jobs {
			if job.running {
				continue
			}
			if job.done {
				delete(s.jobs, domainname)
				continue
			}
			if job.prevRun.IsZero() && job.nextRun.IsZero() {
				if job.opts.Firstrun != nil && job.opts.Firstrun.Seconds > 0 {
					job.nextRun = time.Unix(job.opts.Firstrun.Seconds, 0)
				} else {
					job.prevRun = time.Now()
					job.running = true
					go s.RunJob(domainname, job)
					continue
				}
			}
			if job.prevRun.IsZero() {
				if time.Now().After(job.nextRun) {
					job.prevRun = time.Now()
					job.running = true
					go s.RunJob(domainname, job)
					continue
				}
				continue
			}
			if time.Now().After(job.prevRun.Add(job.frequency)) {
				job.prevRun = time.Now()
				job.running = true
				go s.RunJob(domainname, job)
				continue
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *ideaCrawlerServer) RunJob(subId string, job *Job) {
	var logfile = "/dev/stdout"
	if cliParams.LogPath != "" {
		logfile = path.Join(cliParams.LogPath, "job-"+subId+".log")
	}
	logFP, err := os.Create(logfile)
	if err != nil {
		log.Printf("Unable to create logfile %v. not proceeding with job.", logfile)
		return
	}
	job.log = log.New(logFP, subId+": ", log.LstdFlags|log.Lshortfile)
	job.log.Println("starting job -", subId)
	go func() {
		job.doneListeners = []chan jobDoneSignal{}
	registerDoneLoop:
		for {
			select {
			case vv := <-job.registerDoneListener:
				job.doneListeners = append(job.doneListeners, vv)
			case jds := <-job.doneChan:
				for _, ww := range job.doneListeners {
					ww <- jds
				}
				break registerDoneLoop
			}
		}
	}()
	defer func() {
		close(job.subscriber.sendChan)
		job.running = false

		job.done = true
		job.doneChan <- jobDoneSignal{}
		job.log.Println("stopping job -", subId)
	}()
	job.runNumber += 1

	sendPageHTML := func(ctx *fetchbot.Context, phtml pb.PageHTML) {
		if job.subscriber.connected == false {
			return
		}
		select {
		case <-job.subscriber.stopChan:
			job.subscriber.connected = false
			if ctx != nil && job.opts.CancelOnDisconnect {
				job.log.Printf("Lost client, cancelling queue.\n")
				job.cancelChan <- cancelSignal{}
			} else {
				job.log.Printf("Lost client\n")
			}
			return
		case job.subscriber.sendChan <- phtml:
			return
		}
	}

	mux := fetchbot.NewMux()
	mux.HandleErrors(fetchbot.HandlerFunc(func(ctx *fetchbot.Context, res *http.Response, err error) {
		job.log.Printf("ERR - fetch error : %s\n", err.Error())
		phtml := pb.PageHTML{
			Success:        false,
			Error:          err.Error(),
			Sub:            &pb.Subscription{},
			Url:            res.Request.URL.String(),
			Httpstatuscode: HTTPSTATUS_FETCHBOT_ERROR,
			Content:        []byte{},
			MetaStr:        ctx.Cmd.(CrawlCommand).MetaStr(),
			UrlDepth:       ctx.Cmd.(CrawlCommand).URLDepth(),
		}
		sendPageHTML(ctx, phtml)
		return
	}))
	// Handle GET requests for html responses, to parse the body and enqueue all links as HEAD
	// requests.
	mux.Response().Method("GET").ContentType("text/html").Handler(fetchbot.HandlerFunc(
		func(ctx *fetchbot.Context, res *http.Response, err error) {
			ccmd := ctx.Cmd.(CrawlCommand)
			if ccmd.noCallback == true {
				return
			}
			pageBody, err := ioutil.ReadAll(res.Body)
			if err != nil {
				emsg := fmt.Sprintf("ERR - %s %s - %s", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
				job.log.Println(emsg)
				phtml := pb.PageHTML{
					Success:        false,
					Error:          emsg,
					Sub:            &pb.Subscription{},
					Url:            "",
					Httpstatuscode: HTTPSTATUS_RESPONSE_ERROR,
					Content:        []byte{},
					MetaStr:        "",
					UrlDepth:       ccmd.URLDepth(),
				}
				sendPageHTML(ctx, phtml)
				return
			}

			// check if still logged in
			if job.opts.Login && job.opts.CheckLoginAfterEachPage && job.opts.LoginSuccessCheck != nil {
				doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
				expr, _ := xpath.Compile(job.opts.LoginSuccessCheck.Key) // won't lead to error now, because this is the second time this is happening.

				iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
				iter.MoveNext()
				if strings.ToLower(iter.Current().Value()) != strings.ToLower(job.opts.LoginSuccessCheck.Value) {
					errMsg := fmt.Sprintf("Not logged in anymore: In xpath '%s', expected '%s', but got '%s'. Not continuing.", job.opts.LoginSuccessCheck.Key, job.opts.LoginSuccessCheck.Value, iter.Current().Value())
					job.log.Println(errMsg)
					phtml := pb.PageHTML{
						Success:        false,
						Error:          errMsg,
						Sub:            nil, //no subscription object
						Url:            "",
						Httpstatuscode: HTTPSTATUS_NOLONGER_LOGGED_IN,
						Content:        []byte{},
						MetaStr:        "",
						UrlDepth:       ccmd.URLDepth(),
					}
					sendPageHTML(ctx, phtml)
					job.cancelChan <- cancelSignal{}
					if cliParams.SaveLoginPages != "" {
						err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedout.html"), pageBody, 0755)
						if err != nil {
							job.log.Println("ERR - unable to save loggedout file:", err)
						}
					}
					job.done = true
					return
				}
			}

			// Enqueue all links as HEAD requests, if they match followUrlRegexp
			if job.opts.NoFollow == false && (job.followUrlRegexp == nil || job.followUrlRegexp.MatchString(ctx.Cmd.URL().String()) == true) && (job.opts.Depth < 0 || ccmd.URLDepth() < job.opts.Depth) {
				// Process the body to find the links
				doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(pageBody)))
				if err != nil {
					emsg := fmt.Sprintf("ERR - %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
					job.log.Println(emsg)
					phtml := pb.PageHTML{
						Success:        false,
						Error:          emsg,
						Sub:            &pb.Subscription{},
						Url:            "",
						Httpstatuscode: HTTPSTATUS_FOLLOW_PARSE_ERROR,
						Content:        []byte{},
						MetaStr:        "",
						UrlDepth:       ccmd.URLDepth(),
					}
					sendPageHTML(ctx, phtml)
					return
				}
				job.EnqueueLinks(ctx, doc, ccmd.URLDepth() + 1)
				job.log.Println("Enqueued", ctx.Cmd.URL().String())
			}

			var callbackPage = false

			if len(job.opts.CallbackUrlRegexp) == 0 && len(job.opts.CallbackXpathMatch) == 0 && len(job.opts.CallbackXpathRegexp) == 0 {
				callbackPage = true
			}

			if job.callbackUrlRegexp != nil && job.callbackUrlRegexp.MatchString(ctx.Cmd.URL().String()) == false {
				job.log.Printf("Url '%v' did not match callbackRegexp '%v'\n", ctx.Cmd.URL().String(), job.callbackUrlRegexp)
			} else if job.callbackUrlRegexp != nil {
				callbackPage = true
			}

			if callbackPage == false && len(job.opts.CallbackXpathMatch) > 0 {
				passthru := true
				doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
				for _, xm := range job.opts.CallbackXpathMatch {
					expr, _ := xpath.Compile(xm.Key)

					iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
					iter.MoveNext()
					if iter.Current().Value() != xm.Value {
						passthru = false
						job.log.Printf("Url '%v' did not have '%v' at xpath '%v'\n", ctx.Cmd.URL().String(), xm.Value, xm.Key)
						break
					}
				}
				if passthru {
					callbackPage = true
				}
			}

			if callbackPage == false && len(job.opts.CallbackXpathRegexp) > 0 {
				passthru := true
				doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
				for _, xm := range job.opts.CallbackXpathRegexp {
					expr, _ := xpath.Compile(xm.Key)

					iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
					iter.MoveNext()
					if iter.Current().Value() != xm.Value {
						passthru = false
						job.log.Printf("Url '%v' did not have '%v' at xpath '%v'\n", ctx.Cmd.URL().String(), xm.Value, xm.Key)
						break
					}
				}
				if passthru {
					callbackPage = true
				}
			}

			if callbackPage == false {
				return // no need to send page back to client.
			}
			shippingUrl := ""
			if strings.HasPrefix(ctx.Cmd.URL().Scheme, "crawljs") == true {
				shippingUrl, err = url.QueryUnescape(ctx.Cmd.URL().RawQuery)
				if err != nil {
					job.log.Printf("Unable to UnEscape RawQuery - %v, err: %v\n", ctx.Cmd.URL().RawQuery, err)
					shippingUrl = ctx.Cmd.URL().String()
				}
			} else {
				shippingUrl = ctx.Cmd.URL().String()
			}
			job.log.Println("Shipping", shippingUrl)

			sub := job.sub

			phtml := pb.PageHTML{
				Success:        true,
				Error:          "",
				Sub:            &sub,
				Url:            shippingUrl,
				Httpstatuscode: int32(res.StatusCode),
				Content:        pageBody,
				MetaStr:        ccmd.MetaStr(),
				UrlDepth:       ccmd.URLDepth(),
			}
			sendPageHTML(ctx, phtml)
		}))

	// Handle HEAD requests for html responses coming from the source host - we don't want
	// to crawl links from other hosts.
	mux.Response().Method("HEAD").ContentType("text/html").Handler(fetchbot.HandlerFunc(
		func(ctx *fetchbot.Context, res *http.Response, err error) {
			cmd := CrawlCommand{"GET", ctx.Cmd.URL(), false, ctx.Cmd.(CrawlCommand).MetaStr(), 0}
			if err := ctx.Q.Send(cmd); err != nil {
				job.log.Printf("ERR - %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
			}
		}))

	f := fetchbot.New(mux)
	job.log.Printf("MaxConcurrentRequests=%v\n", int64(job.opts.MaxConcurrentRequests))
	var networkTransport http.RoundTripper = nil
	if job.opts.NetworkIface != "" && cliParams.DialAddress != "" {
		localAddress := &net.TCPAddr{
			IP:   net.ParseIP(cliParams.DialAddress), // a secondary local IP I assigned to me
			Port: 80,
		}
		networkTransport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
				LocalAddr: localAddress,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	if job.opts.Chrome == false && job.opts.Login == false {
		gCurCookieJar, _ := cookiejar.New(nil)
		httpClient := &http.Client{
			Transport:     &httpcache.Transport{networkTransport, diskcache.New("/tmp/ideacache/" + job.sub.Subcode), true},
			CheckRedirect: nil,
			Jar:           gCurCookieJar,
		}
		f.HttpClient = &IdeaCrawlDoer{httpClient, job, semaphore.NewWeighted(int64(job.opts.MaxConcurrentRequests)), s}
	}
	if job.opts.Chrome == false && job.opts.Login == true {
		// create our own httpClient and attach a cookie jar to it,
		// login using that client to the site if required,
		// check if login succeeded,
		// then give that client to fetchbot's fetcher.
		gCurCookieJar, _ := cookiejar.New(nil)
		httpClient := &http.Client{
			Transport:     &httpcache.Transport{networkTransport, diskcache.New("/tmp/ideacache/" + job.sub.Subcode), true},
			CheckRedirect: nil,
			Jar:           gCurCookieJar,
		}
		var payload = make(url.Values)
		if job.opts.LoginParseFields == true {
			afterTime := job.opts.MinDelay
			if afterTime < 5 {
				afterTime = 5
			}
			if job.opts.MaxDelay > job.opts.MinDelay {
				afterTime = int32(<-job.randChan)
				job.log.Printf("Next delay - %v\n", time.Duration(afterTime)*time.Second)
			}
			after := time.After(time.Duration(afterTime) * time.Second)

			httpReq, err := http.NewRequest("GET", job.opts.LoginUrl, nil)
			if err != nil {
				job.log.Println(err)
				return
			}
			httpReq.Header.Set("User-Agent", job.opts.Useragent)

			httpResp, err := httpClient.Do(httpReq)
			if err != nil {
				job.log.Println(err)
				return
			}
			pageBody, err := ioutil.ReadAll(httpResp.Body)
			if err != nil {
				job.log.Println("Unable to read http response:", err)
				return
			}
			if cliParams.SaveLoginPages != "" {
				err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "login.html"), pageBody, 0755)
				if err != nil {
					job.log.Println("ERR - Unable to save login file:", err)
				}
			}

			doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
			for _, kvp := range job.opts.LoginParseXpath {
				expr, err := xpath.Compile(kvp.Value)
				if err != nil {
					job.log.Println(err)
					return
				}

				iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
				iter.MoveNext()
				payload.Set(kvp.Key, iter.Current().Value())
			}
			<-after
		}

		for _, kvp := range job.opts.LoginPayload {
			payload.Set(kvp.Key, kvp.Value)
		}
		httpResp, err := httpClient.PostForm(job.opts.LoginUrl, payload)
		if err != nil {
			job.log.Println(err)
			return
		}
		pageBody, err := ioutil.ReadAll(httpResp.Body)
		if cliParams.SaveLoginPages != "" {
			err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedin.html"), pageBody, 0755)
			if err != nil {
				job.log.Println("ERR - Unable to save loggedin file:", err)
			}

		}
		doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
		if job.opts.LoginSuccessCheck != nil {
			expr, err := xpath.Compile(job.opts.LoginSuccessCheck.Key)
			if err != nil {
				job.log.Println(err)
				return
			}
			iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
			iter.MoveNext()
			if strings.ToLower(iter.Current().Value()) != strings.ToLower(job.opts.LoginSuccessCheck.Value) {
				errMsg := fmt.Sprintf("Login failed: In xpath '%s', expected '%s', but got '%s'. Not proceeding.", job.opts.LoginSuccessCheck.Key, job.opts.LoginSuccessCheck.Value, iter.Current().Value())
				job.log.Println(errMsg)
				phtml := pb.PageHTML{
					Success:        false,
					Error:          errMsg,
					Sub:            nil, //no subscription object
					Url:            "",
					Httpstatuscode: HTTPSTATUS_LOGIN_FAILED,
					Content:        []byte{},
					MetaStr:        "",
				}
				sendPageHTML(nil, phtml)
				return
			}
			phtml := pb.PageHTML{
				Success:        true,
				Error:          "",
				Sub:            nil, //no subscription object
				Url:            "",
				Httpstatuscode: HTTPSTATUS_LOGIN_SUCCESS,
				Content:        []byte{},
				MetaStr:        "",
			}
			sendPageHTML(nil, phtml)
			job.log.Printf("Logged in. Found '%v' in '%v'\n", job.opts.LoginSuccessCheck.Value, job.opts.LoginSuccessCheck.Key)
		}
		f.HttpClient = &IdeaCrawlDoer{httpClient, job, semaphore.NewWeighted(int64(job.opts.MaxConcurrentRequests)), s}
	}

	if job.opts.Chrome == true && job.opts.Login == true {
		job.opts.Impolite = true // Always impolite in Chrome mode.
		if job.opts.ChromeBinary == "" {
			job.opts.ChromeBinary = "/usr/lib64/chromium-browser/headless_shell"
		}
		cl := chromeclient.NewChromeClient(job.opts.ChromeBinary)
		if job.opts.DomLoadTime > 0 {
			cl.SetDomLoadTime(job.opts.DomLoadTime)
		}
		err := cl.Start()
		if err != nil {
			job.log.Println("Unable to start chrome:", err)
			return
		}
		defer cl.Stop()
		urlobj, _ := url.Parse(job.opts.LoginUrl)
		req := &http.Request{
			URL: urlobj,
		}
		loginResp, err := cl.Do(req)
		if err != nil {
			job.log.Println("Http request to chrome failed:", err)
			return
		}
		loginBody, err := ioutil.ReadAll(loginResp.Body)
		if err != nil {
			job.log.Println("Unable to read http response", err)
			return
		}
		if cliParams.SaveLoginPages != "" {
			err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "login.html"), loginBody, 0755)
			if err != nil {
				job.log.Println("ERR - Unable to save login file:", err)
			}
		}
		loginjs := &url.URL{
			Scheme:   "crawljs-jscript",
			Host:     job.opts.LoginUrl,
			Path:     url.PathEscape(job.opts.LoginJS), // js string
			RawQuery: url.QueryEscape(job.opts.LoginUrl),
		}
		loginJsReq := &http.Request{
			URL: loginjs,
		}
		loggedInResp, err := cl.Do(loginJsReq)
		loggedInBody, err := ioutil.ReadAll(loggedInResp.Body)
		if cliParams.SaveLoginPages != "" {
			err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedin.html"), loggedInBody, 0755)
			if err != nil {
				job.log.Println("ERR - Unable to save loggedin file:", err)
			}

		}
		doc, _ := htmlquery.Parse(bytes.NewBuffer(loggedInBody))
		if job.opts.LoginSuccessCheck != nil {
			expr, err := xpath.Compile(job.opts.LoginSuccessCheck.Key)
			if err != nil {
				job.log.Printf("Unable to compile xpath - %v; err:%v\n", job.opts.LoginSuccessCheck.Key, err)
				return
			}
			iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
			iter.MoveNext()
			if iter.Current().Value() != job.opts.LoginSuccessCheck.Value {
				errMsg := fmt.Sprintf("Login failed: In xpath '%s', expected '%s', but got '%s'. Not proceeding.", job.opts.LoginSuccessCheck.Key, job.opts.LoginSuccessCheck.Value, iter.Current().Value())
				job.log.Println(errMsg)
				phtml := pb.PageHTML{
					Success:        false,
					Error:          errMsg,
					Sub:            nil, //no subscription object
					Url:            "",
					Httpstatuscode: HTTPSTATUS_LOGIN_FAILED,
					Content:        []byte{},
					MetaStr:        "",
				}
				sendPageHTML(nil, phtml)
				return
			}
			phtml := pb.PageHTML{
				Success:        true,
				Error:          "",
				Sub:            nil, //no subscription object
				Url:            "",
				Httpstatuscode: HTTPSTATUS_LOGIN_SUCCESS,
				Content:        []byte{},
				MetaStr:        "",
			}
			sendPageHTML(nil, phtml)
			job.log.Printf("Logged in. Found '%v' in '%v'\n", job.opts.LoginSuccessCheck.Value, job.opts.LoginSuccessCheck.Key)
		}
		f.HttpClient = &IdeaCrawlDoer{cl, job, semaphore.NewWeighted(int64(job.opts.MaxConcurrentRequests)), s}
	}

	if job.opts.Chrome == true && job.opts.Login == false {
		job.opts.Impolite = true // Always impolite in Chrome mode.
		if job.opts.ChromeBinary == "" {
			job.opts.ChromeBinary = "/usr/lib64/chromium-browser/headless_shell"
		}
		cl := chromeclient.NewChromeClient(job.opts.ChromeBinary)
		if job.opts.DomLoadTime > 0 {
			cl.SetDomLoadTime(job.opts.DomLoadTime)
		}
		err := cl.Start()
		if err != nil {
			job.log.Println("Unable to start chrome:", err)
			return
		}
		defer cl.Stop()
		f.HttpClient = &IdeaCrawlDoer{cl, job, semaphore.NewWeighted(int64(job.opts.MaxConcurrentRequests)), s}
	}

	f.DisablePoliteness = job.opts.Impolite
	// minimal crawl delay; actual randomized delay is implemented in IdeaCrawlDoer's Do method.
	f.CrawlDelay = 50 * time.Millisecond
	if job.opts.MaxIdleTime < 600 {
		f.WorkerIdleTTL = 600 * time.Second
	} else {
		f.WorkerIdleTTL = time.Duration(job.opts.MaxIdleTime) * time.Second
	}
	f.AutoClose = true
	f.UserAgent = job.opts.Useragent

	//TODO: hipri: create goroutine to listen for new PageRequest objects

	q := f.Start()

	// handle cancel requests
	go func() {
		jobDoneChan := make(chan jobDoneSignal)
		job.registerDoneListener <- jobDoneChan
		select {
		case <-jobDoneChan:
			return
		case <-job.cancelChan:
			q.Cancel()
			job.log.Println("Cancelled job:", subId)
		}
	}()

	// handle stuff coming through the addPage function
	go func() {
		jobDoneChan := make(chan jobDoneSignal)
		job.registerDoneListener <- jobDoneChan
	handlePagesLoop:
		for {
			select {
			case pr := <-job.subscriber.reqChan: // TODO: No URL normalization if added through this method?
				switch pr.Reqtype {
				case pb.PageReqType_GET:
					// TODO:  add error checking for error from Send functions
					cmd, err := CreateCommand("GET", pr.Url, pr.MetaStr, 0)
					if err != nil {
						job.log.Println(err)
						return
					}
					err = q.Send(cmd)
					if err != nil {
						job.log.Println(err)
						return
					}
				case pb.PageReqType_HEAD:
					cmd, err := CreateCommand("HEAD", pr.Url, pr.MetaStr, 0)
					if err != nil {
						job.log.Println(err)
						return
					}
					err = q.Send(cmd)
					if err != nil {
						job.log.Println(err)
						return
					}
				case pb.PageReqType_BUILTINJS:
					prUrl, err := url.Parse(pr.Url)
					if err != nil {
						job.log.Println(err)
						return
					}
					cmd := CrawlCommand{
						method: "GET",
						url: &url.URL{
							Scheme:   "crawljs-builtinjs",
							Host:     prUrl.Host,
							Path:     url.PathEscape(pr.Js), // url command name
							RawQuery: url.QueryEscape(pr.Url),
						},
						noCallback: pr.NoCallback,
						metaStr:    pr.MetaStr,
						urlDepth: 0,
					}
					err = q.Send(cmd)
					if err != nil {
						job.log.Println(err)
						return
					}
				case pb.PageReqType_JSCRIPT:
					prUrl, err := url.Parse(pr.Url)
					if err != nil {
						job.log.Println(err)
						return
					}
					cmd := CrawlCommand{
						method: "GET",
						url: &url.URL{
							Scheme:   "crawljs-jscript",
							Host:     prUrl.Host,
							Path:     url.PathEscape(pr.Js), // url command name
							RawQuery: url.QueryEscape(pr.Url),
						},
						noCallback: pr.NoCallback,
						metaStr:    pr.MetaStr,
						urlDepth: 0,
					}
					err = q.Send(cmd)
					if err != nil {
						job.log.Println(err)
						return
					}
				}
				job.log.Println("Enqueued page:", pr.Url)
			case <-jobDoneChan:
				break handlePagesLoop
			}
		}
	}()
	if len(job.opts.SeedUrl) > 0 {
		job.duplicates[job.opts.SeedUrl] = true
		cmd, err := CreateCommand("GET", job.opts.SeedUrl, "", 0)
		if err != nil {
			job.log.Println(err)
			return
		}
		err = q.Send(cmd)
		if err != nil {
			job.log.Println(err)
			return
		}
	}
	q.Block()
}

func (s *ideaCrawlerServer) AddPages(stream pb.IdeaCrawler_AddPagesServer) error {
	pgreq1, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if pgreq1 == nil {
		emsg := "Received nil pagereq in AddPages.  Exiting AddPages"
		log.Println(emsg)
		return errors.New(emsg)
	}
	if pgreq1.Sub == nil {
		emsg := fmt.Sprintf("Received pagereq with nil sub object. Exiting AddPages.  PageReq - %v", pgreq1)
		log.Println(emsg)
		return errors.New(emsg)
	}
	retChan := make(chan NewJobStatus)
	s.newSubChan <- NewSub{*pgreq1.Sub, retChan}
	njs := <-retChan
	if njs.err != nil {
		return njs.err
	}
	jobDoneChan := make(chan jobDoneSignal)
	njs.registerDoneListener <- jobDoneChan
	reqChan := njs.subscriber.reqChan
	reqChan <- *pgreq1
	log.Printf("Adding new page for job '%v': %v", pgreq1.Sub.Subcode, pgreq1.Url)
	for {
		pgreq, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if pgreq == nil {
			emsg := "Received nil pagereq in AddPages.  Exiting AddPages"
			log.Println(emsg)
			return errors.New(emsg)
		}
		select {
		case <-jobDoneChan:
			return nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
		reqChan <- *pgreq
		log.Printf("Adding new page for job '%v': %v", pgreq.Sub.Subcode, pgreq.Url)
	}
}

func (s *ideaCrawlerServer) CancelJob(ctx context.Context, sub *pb.Subscription) (*pb.Status, error) {
	if sub == nil {
		emsg := "Received nil subscription in CancelJob.  Not canceling anything."
		log.Println(emsg)
		return &pb.Status{false, emsg}, errors.New(emsg)
	}
	log.Println("Cancel request received for job:", sub.Subcode)
	retChan := make(chan NewJobStatus)
	s.newSubChan <- NewSub{*sub, retChan}
	njs := <-retChan
	if njs.err != nil {
		log.Println("ERR - Cancel failed -", njs.err.Error())
		return &pb.Status{false, njs.err.Error()}, njs.err
	}
	njs.cancelChan <- cancelSignal{}
	return &pb.Status{true, ""}, nil
}

func (s *ideaCrawlerServer) AddDomainAndListen(opts *pb.DomainOpt, ostream pb.IdeaCrawler_AddDomainAndListenServer) error {
	retChan := make(chan NewJobStatus)
	s.newJobChan <- NewJob{opts, retChan, true}
	njs := <-retChan
	if njs.err != nil {
		return njs.err
	}
	if njs.subscriber.connected == false {
		return errors.New("Subscriber object not created")
	}
	log.Println("Sending subscription object to client:", njs.sub.Subcode)
	// send an empty pagehtml with just the subscription object,  as soon as job starts.
	err := ostream.Send(&pb.PageHTML{
		Success:        true,
		Error:          "subscription.object",
		Sub:            &njs.sub,
		Url:            "",
		Httpstatuscode: HTTPSTATUS_SUBSCRIPTION,
		Content:        []byte{},
		MetaStr:        "",
	})
	if err != nil {
		log.Printf("Failed to send sub object to client. Cancelling job - %v. Error - %v\n", njs.sub.Subcode, err)
		njs.subscriber.stopChan <- true
		return err
	}

	for pagehtml := range njs.subscriber.sendChan {
		err := ostream.Send(&pagehtml)
		if err != nil {
			log.Printf("Failed to send page back to client. No longer trying - %v. Error - %v\n", njs.sub.Subcode, err)
			njs.subscriber.stopChan <- true
			return err
		}
	}
	return nil
}

func (job *Job) EnqueueLinks(ctx *fetchbot.Context, doc *goquery.Document, urlDepth int32) {
	job.mu.Lock()
	defer job.mu.Unlock()
	var SendMethod = "GET"
	if job.opts.CheckContent == true {
		SendMethod = "HEAD"
	}
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		val, _ := s.Attr("href")
		// Resolve address
		u, err := ctx.Cmd.URL().Parse(val)
		if err != nil {
			job.log.Printf("enqueuelinks: resolve URL %s - %s\n", val, err)
			return
		}
		normFlags := purell.FlagsSafe
		if job.opts.UnsafeNormalizeURL == true {
			normFlags = purell.FlagsSafe | purell.FlagRemoveFragment | purell.FlagRemoveDirectoryIndex
		}
		nurl := purell.NormalizeURL(u, normFlags)
		var reqMatch = true
		var followMatch = true
		if job.callbackUrlRegexp != nil && job.callbackUrlRegexp.MatchString(nurl) == false {
			reqMatch = false
		}
		if job.followUrlRegexp != nil && job.followUrlRegexp.MatchString(nurl) == false {
			followMatch = false
		}
		job.log.Println("Enqueue Status", reqMatch, followMatch, nurl)
		if !reqMatch && !followMatch {
			return
		}
		if !job.duplicates[nurl] {
			if !job.opts.FollowOtherDomains && u.Hostname() != job.domainname {
				job.duplicates[nurl] = true
				return
			}
			cmd, err := CreateCommand(SendMethod, nurl, "", urlDepth)
			if err != nil {
				job.log.Println(err)
				return
			}
			if err := ctx.Q.Send(cmd); err != nil {
				job.log.Println("error: enqueue head %s - %s", nurl, err)
			} else {
				job.duplicates[nurl] = true
			}
		}
	})
}

func newServer(newJobChan chan<- NewJob, newSubChan chan<- NewSub) *ideaCrawlerServer {
	s := new(ideaCrawlerServer)
	s.jobs = make(map[string]*Job)
	s.newJobChan = newJobChan
	s.newSubChan = newSubChan
	return s
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	rand.Seed(time.Now().UTC().UnixNano())

	sflag.Parse(&cliParams)
	if cliParams.LogPath != "" {
		err := os.MkdirAll(cliParams.LogPath, 0755)
		if err != nil {
			panic(err)
		}

		logFP, err := os.Create(path.Join(cliParams.LogPath, "master.log"))
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFP)
	}

	log.Println(cliParams)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", cliParams.ListenAddress, cliParams.ListenPort))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		os.Exit(1)
	}

	defer fmt.Println("Exiting crawler. Bye")
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	newJobChan := make(chan NewJob)
	newSubChan := make(chan NewSub)
	newsrv := newServer(newJobChan, newSubChan)
	pb.RegisterIdeaCrawlerServer(grpcServer, newsrv)
	go newsrv.JobManager(newJobChan, newSubChan)
	grpcServer.Serve(lis)
}
