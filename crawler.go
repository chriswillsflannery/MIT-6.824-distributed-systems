package main

import (
	"fmt"
	"sync"
)

//
// Several solutions to the crawler exercise from the Go tutorial
// https://tour.golang.org/concurrency/10
//

//
// Serial crawler
//

// we don't need to access fetched with a pointer
// because map is passed by reference in go
func Serial(url string, fetcher Fetcher, fetched map[string]bool) {
	if fetched[url] {
		return
	}
	fetched[url] = true
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	for _, u := range urls {
		Serial(u, fetcher, fetched)
	}
	return
}

/* If we update the recursive Serial invocation to use "go" routine,
we could potentially crash the program because the "fetched" map is
shared between multiple simultaneously executing goroutines which
could accidentally overwrite one another.

When we create a goroutine, we create a new goroutine for each URL,
but we're not "waiting" for any of these routines to complete.

The biggest issue here is that the main Serial function returns on line
29, immediately after invoking some child recursive calls to Serial.
This means that it won't wait for those child processes to finsih,
because the main function will finish first.
*/

//
// Concurrent crawler with shared state and Mutex
//

type fetchState struct {
	mu      sync.Mutex
	fetched map[string]bool
}

func (fs *fetchState) testAndSet(url string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	r := fs.fetched[url]
	fs.fetched[url] = true
	return r
}
/*
By creating a lock on fetchState which is passed by reference
to all goroutines, we can ensure that when we create the mutex lock,
all routines trying to access the "fetched" resource will respect
the same lock.

This way, we can ensuree that there is never a race condition between
different threads trying to access the same value in the "fetched"
at the same time.
*/

// we need to access fetchState with a pointer because 
// struct is NOT passed by reference automatically
func ConcurrentMutex(url string, fetcher Fetcher, fs *fetchState) {
	if fs.testAndSet(url) {
		return
	}
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		go func(u string) {
			ConcurrentMutex(u, fetcher, fs)
			done.Done() // decrement wait group counter by 1
		}(u)
	}
	done.Wait() // wait for all spawned goroutines to complete
	return
}
/*
Here, if some subroutine fails, done.Done() might not get called.
We can fix this by deferring execution:
defer done.Done()

Additionally, we need to explicitly pass "u" as an argumetn to the 
self invoked function. This is due to an implementation detail of Go
where loop variables are re-used across iterations. So by the time
the goroutines actually run, it's possible that the for loop has
already completed, and all the goroutines will have a reference to the
final value of "u" in the for loop. so by explicitly passing "u" as an
arg, we make a copy of what "u" was at the time that the goroutine
was invoked.
*/

func makeState() *fetchState {
	return &fetchState{fetched: make(map[string]bool)}
}

//
// Concurrent crawler with channels
//

func worker(url string, ch chan []string, fetcher Fetcher) {
	urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
	} else {
		ch <- urls
	}
}

func coordinator(ch chan []string, fetcher Fetcher) {
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch {
		for _, u := range urls {
			if fetched[u] == false {
				fetched[u] = true
				n += 1
				go worker(u, ch, fetcher)
			}
		}
		n -= 1
		if n == 0 {
			break
		}
	}
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()
	coordinator(ch, fetcher)
}
/*
The purpose of creating channels here is that we can have one "main" thread of execution
which keeps a reference to the "main" mapping of visited URLS. the main thread knows 
which URLS have already been visited, and will only spin up a new worker thread if
the URL which the worker thread needs to process, has not already been visited.

Because the main thread handles lookups against the map, there is no need for a 
mutex lock, since it is guaranteed that there is onyl ever a single thread (the main thread)]
which is accessing the map at a given time.
*/

//
// main
//

func main() {
	fmt.Printf("=== Serial===\n")
	Serial("http://golang.org/", fetcher, make(map[string]bool))

	fmt.Printf("=== ConcurrentMutex ===\n")
	ConcurrentMutex("http://golang.org/", fetcher, makeState())

	fmt.Printf("=== ConcurrentChannel ===\n")
	ConcurrentChannel("http://golang.org/", fetcher)
}

//
// Fetcher
//

type Fetcher interface {
	// Fetch returns a slice of URLs found on the page.
	Fetch(url string) (urls []string, err error)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) ([]string, error) {
	if res, ok := f[url]; ok {
		fmt.Printf("found:   %s\n", url)
		return res.urls, nil
	}
	fmt.Printf("missing: %s\n", url)
	return nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"http://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}