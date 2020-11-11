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
	"fmt"
	"os"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
	"go.etcd.io/etcd/etcd/gopath/src/gopkg.in/cheggaaa/pb.v1"
)

// watchLatencyCmd represents the watch latency command
var watchTxnLatencyCmd = &cobra.Command{
	Use:   "watch-txn-latency",
	Short: "Benchmark watch latency",
	Long: `Benchmarks the latency for watches by measuring
	the latency between writing to a key and receiving the
	associated watch response.`,
	Run: watchTxnLatencyFunc,
}

var (
	watchTxnLTotal     int
	watchLTxnRate      int
	watchTxnLKeySize   int
	watchTxnLValueSize int
)

func init() {
	RootCmd.AddCommand(watchTxnLatencyCmd)
	watchTxnLatencyCmd.Flags().IntVar(&watchTxnLTotal, "total", 10000, "Total number of txn requests")
	watchTxnLatencyCmd.Flags().IntVar(&watchLTxnRate, "txn-rate", 100, "Number of keys to txn per second")
	watchTxnLatencyCmd.Flags().IntVar(&watchTxnLKeySize, "key-size", 32, "Key size of watch response")
	watchTxnLatencyCmd.Flags().IntVar(&watchTxnLValueSize, "val-size", 32, "Value size of watch response")
}

func watchTxnLatencyFunc(cmd *cobra.Command, args []string) {
	key := string(mustRandBytes(watchTxnLKeySize))
	value := string(mustRandBytes(watchTxnLValueSize))

	clients := mustCreateClients(totalClients, totalConns)
	txnClient := mustCreateConn()

	wchs := make([]clientv3.WatchChan, len(clients))
	for i := range wchs {
		wchs[i] = clients[i].Watch(context.TODO(), key)
	}

	bar = pb.New(watchTxnLTotal)
	bar.Format("Bom !")
	bar.Start()

	limiter := rate.NewLimiter(rate.Limit(watchLTxnRate), watchLTxnRate)
	r := newReport()
	rc := r.Run()

	for i := 0; i < watchTxnLTotal; i++ {
		// limit key put as per reqRate
		if err := limiter.Wait(context.TODO()); err != nil {
			break
		}

		var st time.Time
		var wg sync.WaitGroup
		wg.Add(len(clients))
		barrierc := make(chan struct{})
		for _, wch := range wchs {
			ch := wch
			go func() {
				<-barrierc
				<-ch
				r.Results() <- report.Result{Start: st, End: time.Now()}
				wg.Done()
			}()
		}

		if _, err := txnClient.Txn(context.TODO()).Then(clientv3.OpPut(key, value)).Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to Txn for watch latency benchmark: %v\n", err)
			os.Exit(1)
		}

		st = time.Now()
		close(barrierc)
		wg.Wait()
		bar.Increment()
	}

	close(r.Results())
	bar.Finish()
	fmt.Printf("%s", <-rc)
}
