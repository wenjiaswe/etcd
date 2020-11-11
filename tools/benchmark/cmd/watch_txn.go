// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
)

// watchCmd represents the watch command
var watchTxnCmd = &cobra.Command{
	Use:   "watch-txn",
	Short: "Benchmark watch with txn",
	Long: `Benchmark watch tests the performance of processing watch requests and
sending events to watchers. It tests the sending performance by
changing the value of the watched keys with concurrent txn
requests.

During the test, each watcher watches (--total/--watchers) keys

(a watcher might watch on the same key multiple times if
--watched-key-total is small).

Each key is watched by (--total/--watched-key-total) watchers.
`,
	Run: watchTxnFunc,
}

var (
	watchTxnStreams          int
	watchTxnWatchesPerStream int
	watchedTxnKeyTotal       int

	watchTxnRate  int
	watchTxnTotal int
	watchTxnValSize int
	watchTxnPutOpsPerTxn int

	watchTxnKeySize      int
	watchTxnKeySpaceSize int
	watchTxnSeqKeys      bool
)

type watchedTxnKeys struct {
	watched     []string
	numWatchers map[string]int

	watches []clientv3.WatchChan

	// ctx to control all watches
	ctx    context.Context
	cancel context.CancelFunc
}

func init() {
	RootCmd.AddCommand(watchTxnCmd)
	watchTxnCmd.Flags().IntVar(&watchTxnStreams, "streams", 10, "Total watch streams")
	watchTxnCmd.Flags().IntVar(&watchTxnWatchesPerStream, "watch-per-stream", 100, "Total watchers per stream")
	watchTxnCmd.Flags().IntVar(&watchedTxnKeyTotal, "watched-key-total", 1, "Total number of keys to be watched")

	watchTxnCmd.Flags().IntVar(&watchTxnRate, "txn-rate", 0, "Maximum txns per second (0 is no limit)")
	watchTxnCmd.Flags().IntVar(&watchTxnTotal, "txn-total", 1000, "Number of txn requests")

	watchTxnCmd.Flags().IntVar(&watchTxnKeySize, "key-size", 32, "Key size of watch request")
	watchTxnCmd.Flags().IntVar(&watchTxnValSize, "val-size", 8, "Value size of txn put")
	watchTxnCmd.Flags().IntVar(&watchTxnPutOpsPerTxn, "txn-ops", 1, "Number of puts per txn")
	watchTxnCmd.Flags().IntVar(&watchTxnKeySpaceSize, "key-space-size", 1, "Maximum possible keys")
	watchTxnCmd.Flags().BoolVar(&watchTxnSeqKeys, "sequential-keys", false, "Use sequential keys")
}

func watchTxnFunc(cmd *cobra.Command, args []string) {
	if watchKeySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", watchTxnKeySpaceSize)
		os.Exit(1)
	}
	grpcConns := int(totalClients)
	if totalClients > totalConns {
		grpcConns = int(totalConns)
	}
	wantedConns := 1 + (watchStreams / 100)
	if grpcConns < wantedConns {
		fmt.Fprintf(os.Stderr, "warning: grpc limits 100 streams per client connection, have %d but need %d\n", grpcConns, wantedConns)
	}
	clients := mustCreateClients(totalClients, totalConns)
	wk := newTxnWatchedKeys()
	benchMakeTxnWatches(clients, wk)
	benchTxnWatches(clients, wk)
}

func benchMakeTxnWatches(clients []*clientv3.Client, wk *watchedTxnKeys) {
	streams := make([]clientv3.Watcher, watchTxnStreams)
	for i := range streams {
		streams[i] = clientv3.NewWatcher(clients[i%len(clients)])
	}

	keyc := make(chan string, watchTxnStreams)
	bar = pb.New(watchTxnStreams * watchTxnWatchesPerStream)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()
	rch := r.Results()

	wg.Add(len(streams) + 1)
	wc := make(chan []clientv3.WatchChan, len(streams))
	for _, s := range streams {
		go func(s clientv3.Watcher) {
			defer wg.Done()
			var ws []clientv3.WatchChan
			for i := 0; i < watchTxnWatchesPerStream; i++ {
				k := <-keyc
				st := time.Now()
				wch := s.Watch(wk.ctx, k)
				rch <- report.Result{Start: st, End: time.Now()}
				ws = append(ws, wch)
				bar.Increment()
			}
			wc <- ws
		}(s)
	}
	go func() {
		defer func() {
			close(keyc)
			wg.Done()
		}()
		for i := 0; i < watchTxnStreams*watchTxnWatchesPerStream; i++ {
			key := wk.watched[i%len(wk.watched)]
			keyc <- key
			wk.numWatchers[key]++
		}
	}()

	rc := r.Run()
	wg.Wait()
	bar.Finish()
	close(r.Results())
	fmt.Printf("Watch creation summary:\n%s", <-rc)

	for i := 0; i < len(streams); i++ {
		wk.watches = append(wk.watches, (<-wc)...)
	}
}

func newTxnWatchedKeys() *watchedTxnKeys {
	watched := make([]string, watchedTxnKeyTotal)
	for i := range watched {
		k := make([]byte, watchTxnKeySize)
		if watchTxnSeqKeys {
			binary.PutVarint(k, int64(i%watchTxnKeySpaceSize))
		} else {
			binary.PutVarint(k, int64(rand.Intn(watchTxnKeySpaceSize)))
		}
		watched[i] = string(k)
	}
	ctx, cancel := context.WithCancel(context.TODO())
	return &watchedTxnKeys{
		watched:     watched,
		numWatchers: make(map[string]int),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func benchTxnWatches(clients []*clientv3.Client, wk *watchedTxnKeys) {
	eventsTotal := 0
	for i := 0; i < watchTxnTotal; i++ {
		eventsTotal += wk.numWatchers[wk.watched[i%len(wk.watched)]]
	}

	bar = pb.New(eventsTotal)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()

	wg.Add(len(wk.watches))
	nrRxed := int32(eventsTotal)
	for _, w := range wk.watches {
		go func(wc clientv3.WatchChan) {
			defer wg.Done()
			recvWatchTxnChan(wc, r.Results(), &nrRxed)
			wk.cancel()
		}(w)
	}

	txnreqc := make(chan []clientv3.Op, len(clients))
	_, v := make([]byte, watchTxnKeySpaceSize), string(mustRandBytes(watchTxnValSize))
	go func() {
		for i := 0; i < watchTxnTotal; i++ {
			ops := make([]clientv3.Op, watchTxnPutOpsPerTxn)
			for j := 0; j < watchTxnPutOpsPerTxn; j++ {
				ops[j] = clientv3.OpPut(wk.watched[i%(len(wk.watched))], v)
			}
			txnreqc <- ops
		}
		close(txnreqc)
	}()

	limit := rate.NewLimiter(rate.Limit(watchTxnRate), 1)
	for _, cc := range clients {
		go func(c *clientv3.Client) {
			for ops := range txnreqc {
				if err := limit.Wait(context.TODO()); err != nil {
					panic(err)
				}
				if _, err := c.Txn(context.TODO()).Then(ops...).Commit(); err != nil {
					panic(err)
				}
			}
		}(cc)
	}

	rc := r.Run()
	wg.Wait()
	bar.Finish()
	close(r.Results())
	fmt.Printf("Watch events received summary:\n%s", <-rc)

}

func recvWatchTxnChan(wch clientv3.WatchChan, results chan<- report.Result, nrRxed *int32) {
	for r := range wch {
		st := time.Now()
		for range r.Events {
			results <- report.Result{Start: st, End: time.Now()}
			bar.Increment()
			if atomic.AddInt32(nrRxed, -1) <= 0 {
				return
			}
		}
	}
}
