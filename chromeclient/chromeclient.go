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

package chromeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mafredri/cdp"
	"github.com/mafredri/cdp/devtool"
	"github.com/mafredri/cdp/protocol/dom"
	"github.com/mafredri/cdp/protocol/network"
	"github.com/mafredri/cdp/protocol/page"
	"github.com/mafredri/cdp/protocol/runtime"
	"github.com/mafredri/cdp/rpcc"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type ChromeClient struct {
	cpath       string // path to browser binary
	devt        *devtool.DevTools
	pageTgt     *devtool.Target
	conn        *rpcc.Conn
	c           *cdp.Client
	ctx         context.Context
	cancel      context.CancelFunc
	chromeCmd   *exec.Cmd
	domLoadTime time.Duration
	rspRcvd     network.ResponseReceivedClient
	started     bool
	mu          sync.Mutex
}

func NewChromeClient(path string) *ChromeClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChromeClient{
		cpath:       path,
		devt:        nil,
		chromeCmd:   nil,
		ctx:         ctx,
		cancel:      cancel,
		domLoadTime: 5 * time.Second, // default 5 secs for page to load in browser before we get dump.
	}
}

func (cc *ChromeClient) SetDomLoadTime(secs int32) {
	cc.domLoadTime = time.Duration(secs) * time.Second
}

func (cc *ChromeClient) Stop() {
	cc.conn.Close()
	cc.cancel()
	err := cc.chromeCmd.Process.Kill()
	if err != nil {
		log.Println(err)
	}
}

func (cc *ChromeClient) Start() error {
	var err error
	cc.chromeCmd = exec.Command(cc.cpath, "--headless", "--disable-gpu", "--remote-debugging-port=9222")
	err = cc.chromeCmd.Start()
	time.Sleep(3 * time.Second) // TODO: make customizable. give a few seconds for browser to start.
	if err != nil {
		log.Panic("Unable to start chrome browser in path '" + cc.cpath + "'. Error - " + err.Error())
	}

	cc.devt = devtool.New("http://localhost:9222")
	cc.pageTgt, err = cc.devt.Get(cc.ctx, devtool.Page)
	if err != nil {
		log.Println(err)
		return err
	}

	cc.conn, err = rpcc.DialContext(cc.ctx, cc.pageTgt.WebSocketDebuggerURL)
	if err != nil {
		log.Println(err)
		return err
	}

	// Create a new CDP Client that uses conn.
	cc.c = cdp.NewClient(cc.conn)

	if err = runBatch(
		// Enable all the domain events that we're interested in.
		func() error { return cc.c.DOM.Enable(cc.ctx) },
		func() error { return cc.c.Network.Enable(cc.ctx, nil) },
		func() error { return cc.c.Page.Enable(cc.ctx) },
		func() error { return cc.c.Runtime.Enable(cc.ctx) },
	); err != nil {
		log.Panic(err)
	}

	cc.rspRcvd, err = cc.c.Network.ResponseReceived(cc.ctx)
	if err != nil {
		log.Panic(err)
	}
	return err
}

// "http://" types,  or "crawljs-builtinjs://<hostname>/<js>?<url> or
//                      "crawljs-jscript://<hostname>/<builtin OP name>"
func (cc *ChromeClient) Do(req *http.Request) (resp *http.Response, err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if strings.HasPrefix(req.URL.Scheme, "crawljs") {
		return cc.doJS(req)
	}
	return cc.doNavigate(req)
}

func (cc *ChromeClient) doNavigate(req *http.Request) (resp *http.Response, err error) {
	var url = req.URL.String()
	log.Printf("Chrome doing: %v\n", url)
	domLoadTimer := time.After(cc.domLoadTime)
	err = navigate(cc.ctx, cc.c.Page, url, cc.domLoadTime)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	<-domLoadTimer
	navhist, _ := cc.c.Page.GetNavigationHistory(cc.ctx)

	<-cc.rspRcvd.Ready()
	rsp, err := cc.rspRcvd.Recv()
	currUrl := navhist.Entries[navhist.CurrentIndex].URL
	log.Printf("Current URL: %v\n", currUrl)
	for {
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if rsp.Response.URL == currUrl || rsp.Response.URL+"/" == currUrl || rsp.Response.URL == currUrl+"/" {
			break
		}
		<-cc.rspRcvd.Ready()
		rsp, err = cc.rspRcvd.Recv()
	}

	doc, err := cc.c.DOM.GetDocument(cc.ctx, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ohtml, err := cc.c.DOM.GetOuterHTML(cc.ctx, &dom.GetOuterHTMLArgs{
		NodeID: &doc.Root.NodeID,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}
	resp1 := &http.Response{
		StatusCode: int(rsp.Response.Status),
		Status:     rsp.Response.StatusText,
		Body:       ioutil.NopCloser(strings.NewReader(ohtml.OuterHTML)),
		Request:    req,
		Header:     make(http.Header),
	}
	rspHdrs, _ := rsp.Response.Headers.Map()
	for kk, vv := range rspHdrs {
		if strings.ToLower(kk) == "content-type" {
			resp1.Header.Set("Content-Type", vv)
			break
		}
	}
	return resp1, nil
}

func (cc *ChromeClient) doJS(req *http.Request) (resp *http.Response, err error) {
	domLoadTimer := time.After(cc.domLoadTime)
	jscommand, err := url.PathUnescape(req.URL.Path)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	tgtUrl, err := url.QueryUnescape(req.URL.RawQuery)
	navhist, _ := cc.c.Page.GetNavigationHistory(cc.ctx)
	currUrl := navhist.Entries[navhist.CurrentIndex].URL

	if tgtUrl != currUrl && tgtUrl+"/" != currUrl && tgtUrl != currUrl+"/" {
		domNavigateTimer := time.After(cc.domLoadTime)
		log.Printf("Navigating to %v\n", tgtUrl)
		tgtURL, err := url.Parse(tgtUrl)
		if err != nil {
			log.Printf("doJS: unable to parse tgt url: %v\n", err)
			return nil, err
		}

		navRsp, err := cc.doNavigate(&http.Request{
			URL: tgtURL,
		})
		if err != nil {
			log.Printf("Navigation failed: %v\n", err)
			return nil, err
		}
		if navRsp.StatusCode != 200 {
			log.Printf("HTTP Status Code was: %v;  Use AddPage in chrome mode instead, to get page sent back anyway.")
			return nil, err
		}
		<-domNavigateTimer
	}

	if strings.HasSuffix(req.URL.Scheme, "builtinjs") {
		switch jscommand {
		case "/scrollToEnd":
			expression := `window.scrollTo(0, document.body.scrollHeight)`
			evalArgs := runtime.NewEvaluateArgs(expression)
			_, err := cc.c.Runtime.Evaluate(cc.ctx, evalArgs)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		case "/infiniteScrollToEnd":
			expression := `new Promise((resolve, reject) => {
                                   prevHeight=document.body.scrollHeight;
                                   window.scrollTo(0, document.body.scrollHeight);
                                   setTimeout(() => {
                                       newHeight=document.body.scrollHeight;
                                       resolve({"O": prevHeight, "N": newHeight});
                                   }, ` + strconv.Itoa(int(cc.domLoadTime/time.Millisecond)) + `);
                               });
`
			for {
				evalArgs := runtime.NewEvaluateArgs(expression).SetAwaitPromise(true).SetReturnByValue(true)
				eval, err := cc.c.Runtime.Evaluate(cc.ctx, evalArgs)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				heights := &struct {
					O int
					N int
				}{}
				if err = json.Unmarshal(eval.Result.Value, &heights); err != nil {
					log.Println(err)
					return nil, err
				}
				if heights.O == heights.N {
					log.Printf("Old height = new height = %v. We're probably done scrolling.\n", heights.O)
					break
				}
				log.Printf("Old height: %v; New height: %v; Continuing to scroll down.\n", heights.O, heights.N)
			}
		}
	} else if strings.HasSuffix(req.URL.Scheme, "jscript") {
		evalArgs := runtime.NewEvaluateArgs(jscommand)
		_, err := cc.c.Runtime.Evaluate(cc.ctx, evalArgs)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	<-domLoadTimer
	doc, err := cc.c.DOM.GetDocument(cc.ctx, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ohtml, err := cc.c.DOM.GetOuterHTML(cc.ctx, &dom.GetOuterHTMLArgs{
		NodeID: &doc.Root.NodeID,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}
	navhist, _ = cc.c.Page.GetNavigationHistory(cc.ctx)
	currURL, err := url.Parse(navhist.Entries[navhist.CurrentIndex].URL)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.URL = currURL
	resp1 := &http.Response{
		StatusCode: 900,
		Status:     "",
		Body:       ioutil.NopCloser(strings.NewReader(ohtml.OuterHTML)),
		Request:    req,
		Header:     make(http.Header),
	}
	resp1.Header.Set("Content-Type", "text/html")
	return resp1, nil
}

func navigate(ctx context.Context, pageClient cdp.Page, url string, timeout time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	// Make sure Page events are enabled.
	err := pageClient.Enable(ctx)
	if err != nil {
		return err
	}

	// Open client for DOMContentEventFired to block until DOM has fully loaded.
	domContentEventFired, err := pageClient.DOMContentEventFired(ctx)
	if err != nil {
		return err
	}
	defer domContentEventFired.Close()

	_, err = pageClient.Navigate(ctx, page.NewNavigateArgs(url))
	if err != nil {
		return err
	}

	_, err = domContentEventFired.Recv()
	return err
}

// runBatchFunc is the function signature for runBatch.
type runBatchFunc func() error

// runBatch runs all functions simultaneously and waits until
// execution has completed or an error is encountered.
func runBatch(fn ...runBatchFunc) error {
	eg := errgroup.Group{}
	for _, f := range fn {
		eg.Go(f)
	}
	return eg.Wait()
}

func zmain() {
	cl := NewChromeClient("/usr/lib64/chromium-browser/headless_shell")
	cl.Start()
	defer cl.Stop()
	urlobj, _ := url.Parse("http://books.toscrape.com/")
	req := &http.Request{
		URL: urlobj,
	}
	r, err := cl.Do(req)
	pagebody, err := ioutil.ReadAll(r.Body)

	fmt.Println(err, string(pagebody))

	urlobj, _ = url.Parse("http://quotes.toscrape.com/")
	req = &http.Request{
		URL: urlobj,
	}
	r, err = cl.Do(req)
	pagebody, err = ioutil.ReadAll(r.Body)
	fmt.Println(err, string(pagebody))
}
