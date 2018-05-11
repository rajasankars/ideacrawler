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

package prefetchurl

/*
This package is used to fetch links of js, css, image sources associated with a given pageContent to replicate browser's function.
*/

import (
	"bytes"
	"github.com/PuerkitoBio/goquery"
	"io/ioutil"
	"net/url"
	"regexp"
	"strings"
)

func validateLink(pageURL *url.URL, link string) (string, error) {
	var validLinks = regexp.MustCompile(`^*\.\w`)
	var absoluteLink string
	var err error
	linkSplit := strings.Split(link, "/")
	if validLinks.MatchString(linkSplit[len(linkSplit)-1]) {
		if !(strings.HasSuffix(link, ".xml")) {
			if strings.HasPrefix(link, "http") {
				absoluteLink = link
			} else {
				absoluteUrl, err := pageURL.Parse(link)
				if err != nil {
					return absoluteLink, err
				}
				absoluteLink = absoluteUrl.String()
			}
		}
	}
	return absoluteLink, err
}

func getURLFromHTML(doc *goquery.Document) ([]string, error) {
	var err error
	var findLinks []string
	var prefetchLinks []string
	// Get js sources link
	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		jsSrc := strings.TrimSpace(s.AttrOr("src", ""))
		if jsSrc != "" {
			findLinks = append(findLinks, jsSrc)
		}
		jsLinkHref := strings.TrimSpace(s.Find("link").First().AttrOr("href", ""))
		if jsLinkHref != "" {
			findLinks = append(findLinks, jsLinkHref)
		}
	})

	// Get css sources link
	doc.Find("link").Each(func(i int, s *goquery.Selection) {
		if strings.TrimSpace(s.AttrOr("rel", "")) != "prefetch" {
			linkHref := strings.TrimSpace(s.AttrOr("href", ""))
			if linkHref != "" {
				findLinks = append(findLinks, linkHref)
			}
		}
	})

	// Get img sources link
	doc.Find("img").Each(func(i int, s *goquery.Selection) {
		imgDataSrc := strings.TrimSpace(s.AttrOr("data-img-src", ""))
		if imgDataSrc != "" {
			findLinks = append(findLinks, imgDataSrc)
		}
		imgSrc := strings.TrimSpace(s.AttrOr("src", ""))
		if imgSrc != "" {
			findLinks = append(findLinks, imgSrc)
		}
	})

	for _, eachLink := range findLinks {
		absolutelink, err := validateLink(doc.Url, eachLink)
		if err != nil {
			return nil, err
		}
		if absolutelink != "" {
			prefetchLinks = append(prefetchLinks, absolutelink)
		}
	}
	return prefetchLinks, err
}

func getURLFromCSS(doc *goquery.Document) ([]string, error) {
	var err error
	var prefetchLinks []string
	// Get source urls embedded with css file
	regexpr := regexp.MustCompile(`url\(['"]?([^\)\(]+?)['"]?\)`)
	cssContent := doc.Text()
	if cssContent == "" {
		return prefetchLinks, err
	}
	matchStrings := regexpr.FindAllStringSubmatch(cssContent, -1)
	for _, urlStringArr := range matchStrings {
		url := urlStringArr[1]
		if url != "" {
			absolutelink, err := validateLink(doc.Url, url)
			if err != nil {
				return nil, err
			}
			if absolutelink != "" {
				prefetchLinks = append(prefetchLinks, absolutelink)
			}
		}
	}
	return prefetchLinks, err
}

func GetPrefetchURLs(pageContent []byte, pageURLStr string) ([]string, error) {
	var err error
	var prefetchLinks []string
	fileReader := bytes.NewReader(pageContent)
	doc, err := goquery.NewDocumentFromReader(fileReader)
	if err != nil {
		return nil, err
	}
	pageURL, err := url.Parse(pageURLStr)
	if err != nil {
		return nil, err
	}
	doc.Url = pageURL

	// Check that the given pageURL is css or not. if css, fetch embedded url with css pageContent.
	if strings.HasSuffix(pageURLStr, ".css") {
		prefetchLinks, err = getURLFromCSS(doc)
		if err != nil {
			return nil, err
		}
	} else {
		prefetchLinks, err = getURLFromHTML(doc)
		if err != nil {
			return nil, err
		}
	}
	return prefetchLinks, err
}

func GetPrefetchURLsTest(htmlFile, pageURLStr string) ([]string, error) {
	var err error
	var prefetchLinks []string
	fileBytes, err := ioutil.ReadFile(htmlFile)
	if err != nil {
		return nil, err
	}
	prefetchLinks, err = GetPrefetchURLs(fileBytes, pageURLStr)
	return prefetchLinks, err
}
