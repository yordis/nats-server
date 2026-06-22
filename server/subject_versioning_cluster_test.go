// Copyright 2012-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_store_tests && !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamClusterSubjectVersioningReplicatedPublish(t *testing.T) {
	for _, storage := range []StorageType{FileStorage, MemoryStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			c := createSubjectVersioningCluster(t, "SVR3", 3)
			defer c.shutdown()

			nc, js := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_CLUSTER_%s", storage), storage)
			cfg.Replicas = 3
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			first := nats.NewMsg("events.order.123.created")
			first.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			first.Header.Set(JSMsgId, "replicated-dedupe")
			firstAck := requestSubjectVersioningPubAck(t, nc, first)
			require_NotNil(t, firstAck.SubjectVersion)
			require_Equal(t, *firstAck.SubjectVersion, uint64(0))
			require_Equal(t, firstAck.SubjectVersionKey, "events.order.123")

			second := nats.NewMsg("events.order.123.cancelled")
			second.Header.Set(JSExpectedLastSubjectVer, "0")
			secondAck := requestSubjectVersioningPubAck(t, nc, second)
			require_NotNil(t, secondAck.SubjectVersion)
			require_Equal(t, *secondAck.SubjectVersion, uint64(1))
			require_Equal(t, secondAck.SubjectVersionKey, "events.order.123")

			duplicate := nats.NewMsg("events.order.123.created")
			duplicate.Header.Set(JSMsgId, "replicated-dedupe")
			duplicateAck := requestSubjectVersioningPubAck(t, nc, duplicate)
			require_True(t, duplicateAck.Duplicate)
			require_NotNil(t, duplicateAck.SubjectVersion)
			require_Equal(t, *duplicateAck.SubjectVersion, uint64(0))
			require_Equal(t, duplicateAck.SubjectVersionKey, "events.order.123")
			require_Equal(t, duplicateAck.Sequence, firstAck.Sequence)

			bad := nats.NewMsg("events.order.123.shipped")
			bad.Header.Set(JSExpectedLastSubjectVer, "0")
			_, err = js.PublishMsg(bad)
			require_Error(t, err, NewJSStreamWrongLastSubjectVersionError("1"))
		})
	}
}

func TestJetStreamClusterSubjectVersioningConcurrentPublish(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVCONCURRENT", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_CONCURRENT", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	type publishResult struct {
		key     string
		version uint64
		err     error
	}

	publish := func(subject string, results chan<- publishResult) {
		msg := nats.NewMsg(subject)
		ack, err := requestSubjectVersioningPubAckResponse(nc, msg)
		if err != nil {
			results <- publishResult{err: err}
			return
		}
		if ack.SubjectVersion == nil {
			results <- publishResult{err: fmt.Errorf("missing subject version for %s", subject)}
			return
		}
		results <- publishResult{
			key:     ack.SubjectVersionKey,
			version: *ack.SubjectVersion,
		}
	}

	checkContiguous := func(results []publishResult, key string, want int) {
		t.Helper()

		versions := make([]uint64, 0, want)
		seen := make(map[uint64]struct{}, want)
		for _, result := range results {
			if result.key == key {
				if _, dup := seen[result.version]; dup {
					t.Fatalf("duplicate subject version %d assigned for namespace %q", result.version, key)
				}
				seen[result.version] = struct{}{}
				versions = append(versions, result.version)
			}
		}
		require_Len(t, len(versions), want)
		sort.Slice(versions, func(i, j int) bool {
			return versions[i] < versions[j]
		})
		for i, version := range versions {
			require_Equal(t, version, uint64(i))
		}
	}

	const sameNamespacePublishes = 32
	results := make(chan publishResult, sameNamespacePublishes)
	var wg sync.WaitGroup
	for i := range sameNamespacePublishes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			publish(fmt.Sprintf("events.order.123.step%d", i), results)
		}(i)
	}
	wg.Wait()
	close(results)

	sameNamespaceResults := make([]publishResult, 0, sameNamespacePublishes)
	for result := range results {
		require_NoError(t, result.err)
		sameNamespaceResults = append(sameNamespaceResults, result)
	}
	checkContiguous(sameNamespaceResults, "events.order.123", sameNamespacePublishes)

	const publishesPerNamespace = 16
	results = make(chan publishResult, publishesPerNamespace*2)
	for i := range publishesPerNamespace {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			publish(fmt.Sprintf("events.order.456.step%d", i), results)
		}(i)
		go func(i int) {
			defer wg.Done()
			publish(fmt.Sprintf("events.invoice.900.step%d", i), results)
		}(i)
	}
	wg.Wait()
	close(results)

	multipleNamespaceResults := make([]publishResult, 0, publishesPerNamespace*2)
	for result := range results {
		require_NoError(t, result.err)
		multipleNamespaceResults = append(multipleNamespaceResults, result)
	}
	checkContiguous(multipleNamespaceResults, "events.order.456", publishesPerNamespace)
	checkContiguous(multipleNamespaceResults, "events.invoice.900", publishesPerNamespace)
}

func TestJetStreamClusterAtomicBatchSubjectVersioningExpectedVersionFirstOnly(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVBATCH", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_BATCH", FileStorage)
	cfg.Replicas = 3
	cfg.AllowAtomicPublish = true
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	seed := nats.NewMsg("events.order.123.created")
	seed.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
	seedAck := requestSubjectVersioningPubAck(t, nc, seed)
	require_NotNil(t, seedAck.SubjectVersion)
	require_Equal(t, *seedAck.SubjectVersion, uint64(0))

	first := nats.NewMsg("events.order.123.updated")
	first.Header.Set(JSExpectedLastSubjectVer, "0")
	first.Header.Set(JSBatchId, "uuid")
	first.Header.Set(JSBatchSeq, "1")
	require_NoError(t, nc.PublishMsg(first))

	second := nats.NewMsg("events.order.123.cancelled")
	second.Header.Set(JSExpectedLastSubjectVer, "1")
	second.Header.Set(JSBatchId, "uuid")
	second.Header.Set(JSBatchSeq, "2")
	second.Header.Set(JSBatchCommit, "1")
	respMsg, err := nc.RequestMsg(second, time.Second)
	require_NoError(t, err)

	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(respMsg.Data, &pubAck))
	require_NotNil(t, pubAck.Error)
	require_Error(t, pubAck.Error, NewJSStreamWrongLastSubjectVersionConstantError())

	next := nats.NewMsg("events.order.123.updated")
	next.Header.Set(JSExpectedLastSubjectVer, "0")
	nextAck := requestSubjectVersioningPubAck(t, nc, next)
	require_NotNil(t, nextAck.SubjectVersion)
	require_Equal(t, *nextAck.SubjectVersion, uint64(1))
	require_Equal(t, nextAck.SubjectVersionKey, "events.order.123")
}

func TestJetStreamClusterAtomicBatchSubjectVersioningRollbackPreservesCounters(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVROLLBACK", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_ROLLBACK", FileStorage)
	cfg.Replicas = 3
	cfg.AllowAtomicPublish = true
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	// Seed both namespaces at version 0 outside the batch.
	seedA := nats.NewMsg("events.order.123.created")
	seedAAck := requestSubjectVersioningPubAck(t, nc, seedA)
	require_NotNil(t, seedAAck.SubjectVersion)
	require_Equal(t, *seedAAck.SubjectVersion, uint64(0))

	seedB := nats.NewMsg("events.order.456.created")
	seedBAck := requestSubjectVersioningPubAck(t, nc, seedB)
	require_NotNil(t, seedBAck.SubjectVersion)
	require_Equal(t, *seedBAck.SubjectVersion, uint64(0))

	// Atomic batch of three messages where the second message references a
	// namespace at the wrong expected version. The whole batch must be rolled
	// back, including the bumps that would have been assigned to the OTHER
	// namespace touched by the batch.
	first := nats.NewMsg("events.order.123.updated")
	first.Header.Set(JSExpectedLastSubjectVer, "0")
	first.Header.Set(JSBatchId, "rollback-batch")
	first.Header.Set(JSBatchSeq, "1")
	require_NoError(t, nc.PublishMsg(first))

	second := nats.NewMsg("events.order.456.updated")
	second.Header.Set(JSExpectedLastSubjectVer, "99") // wrong on purpose
	second.Header.Set(JSBatchId, "rollback-batch")
	second.Header.Set(JSBatchSeq, "2")
	require_NoError(t, nc.PublishMsg(second))

	third := nats.NewMsg("events.order.123.cancelled")
	third.Header.Set(JSExpectedLastSubjectVer, "1")
	third.Header.Set(JSBatchId, "rollback-batch")
	third.Header.Set(JSBatchSeq, "3")
	third.Header.Set(JSBatchCommit, "1")
	respMsg, err := nc.RequestMsg(third, time.Second)
	require_NoError(t, err)

	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(respMsg.Data, &pubAck))
	require_NotNil(t, pubAck.Error)

	// If the batch rolled back correctly, BOTH namespace counters are still at
	// version 0 — proven by publishing with no_stream-equivalent preconditions
	// derived from the seed values.
	probeA := nats.NewMsg("events.order.123.recovery")
	probeA.Header.Set(JSExpectedLastSubjectVer, "0")
	probeAAck := requestSubjectVersioningPubAck(t, nc, probeA)
	require_NotNil(t, probeAAck.SubjectVersion)
	require_Equal(t, *probeAAck.SubjectVersion, uint64(1))
	require_Equal(t, probeAAck.SubjectVersionKey, "events.order.123")

	probeB := nats.NewMsg("events.order.456.recovery")
	probeB.Header.Set(JSExpectedLastSubjectVer, "0")
	probeBAck := requestSubjectVersioningPubAck(t, nc, probeB)
	require_NotNil(t, probeBAck.SubjectVersion)
	require_Equal(t, *probeBAck.SubjectVersion, uint64(1))
	require_Equal(t, probeBAck.SubjectVersionKey, "events.order.456")
}

func TestJetStreamClusterAtomicBatchSubjectVersioningRejectsReservedHeaders(t *testing.T) {
	headers := []string{JSSubjectVersion, JSSubjectVersionKey}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			c := createSubjectVersioningCluster(t, "SVABRESH", 3)
			defer c.shutdown()

			nc, _ := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig("SV_BATCH_RESERVED_"+header, FileStorage)
			cfg.Replicas = 3
			cfg.AllowAtomicPublish = true
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			msg := nats.NewMsg("events.order.123.created")
			msg.Header.Set(header, "99")
			msg.Header.Set(JSBatchId, "uuid")
			msg.Header.Set(JSBatchSeq, "1")
			msg.Header.Set(JSBatchCommit, "1")
			respMsg, err := nc.RequestMsg(msg, time.Second)
			require_NoError(t, err)

			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(respMsg.Data, &pubAck))
			require_NotNil(t, pubAck.Error)
			require_Error(t, pubAck.Error, NewJSStreamSubjectVersionHeaderServerManagedError(header))
		})
	}
}

func TestJetStreamClusterSubjectVersioningSurvivesLeaderChange(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVFAIL", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_FAILOVER", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	first := nats.NewMsg("events.order.123.created")
	first.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
	first.Header.Set(JSMsgId, "failover-dedupe")
	firstAck := requestSubjectVersioningPubAck(t, nc, first)
	require_NotNil(t, firstAck.SubjectVersion)
	require_Equal(t, *firstAck.SubjectVersion, uint64(0))

	second := nats.NewMsg("events.order.123.cancelled")
	second.Header.Set(JSExpectedLastSubjectVer, "0")
	secondAck := requestSubjectVersioningPubAck(t, nc, second)
	require_NotNil(t, secondAck.SubjectVersion)
	require_Equal(t, *secondAck.SubjectVersion, uint64(1))

	currentLeader := c.streamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, currentLeader)
	nextLeader := c.randomNonStreamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, nextLeader)

	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: nextLeader.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, cfg.Name), data, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	require_Equal(t, c.streamLeader(globalAccountName, cfg.Name), nextLeader)

	third := nats.NewMsg("events.order.123.shipped")
	third.Header.Set(JSExpectedLastSubjectVer, "1")
	thirdAck := requestSubjectVersioningPubAck(t, nc, third)
	require_NotNil(t, thirdAck.SubjectVersion)
	require_Equal(t, *thirdAck.SubjectVersion, uint64(2))
	require_Equal(t, thirdAck.SubjectVersionKey, "events.order.123")

	duplicate := nats.NewMsg("events.order.123.created")
	duplicate.Header.Set(JSMsgId, "failover-dedupe")
	duplicateAck := requestSubjectVersioningPubAck(t, nc, duplicate)
	require_True(t, duplicateAck.Duplicate)
	require_NotNil(t, duplicateAck.SubjectVersion)
	require_Equal(t, *duplicateAck.SubjectVersion, uint64(0))
	require_Equal(t, duplicateAck.SubjectVersionKey, "events.order.123")
	require_Equal(t, duplicateAck.Sequence, firstAck.Sequence)
	require_NotEqual(t, currentLeader, c.streamLeader(globalAccountName, cfg.Name))
}

// TestJetStreamClusterSubjectVersioningHealthyClusterConvergesSVS asserts the
// foundational property the rolling-upgrade story depends on: in a healthy
// R3 cluster, every replica's svs state is byte-for-byte identical for every
// tracked namespace after a sequence of publishes. If this invariant ever
// breaks, no upgrade scenario can be safe.
func TestJetStreamClusterSubjectVersioningHealthyClusterConvergesSVS(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVCONV", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_CONVERGE", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	expectations := map[string]uint64{
		"events.order.123":   0,
		"events.invoice.900": 0,
	}
	publish := func(t *testing.T, key, leaf string) {
		t.Helper()
		msg := nats.NewMsg(key + "." + leaf)
		ack := requestSubjectVersioningPubAck(t, nc, msg)
		require_NotNil(t, ack.SubjectVersion)
		require_Equal(t, ack.SubjectVersionKey, key)
		require_Equal(t, *ack.SubjectVersion, expectations[key])
		expectations[key]++
	}

	for i := 0; i < 4; i++ {
		publish(t, "events.order.123", fmt.Sprintf("step%d", i))
		publish(t, "events.invoice.900", fmt.Sprintf("step%d", i))
	}

	checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
		for _, s := range c.servers {
			mset, err := s.GlobalAccount().lookupStream(cfg.Name)
			if err != nil {
				return err
			}
			var state StreamState
			mset.store.FastState(&state)
			if state.LastSeq != 8 {
				return fmt.Errorf("%s at seq %d, want 8", s.Name(), state.LastSeq)
			}
		}
		return nil
	})

	for _, s := range c.servers {
		mset, err := s.GlobalAccount().lookupStream(cfg.Name)
		require_NoError(t, err)
		fs, ok := mset.store.(*fileStore)
		require_True(t, ok)
		fs.mu.RLock()
		for key, lastVersion := range expectations {
			entry, found := fs.svs.Find([]byte(key))
			if !found {
				fs.mu.RUnlock()
				t.Fatalf("%s missing svs entry for %s", s.Name(), key)
			}
			require_Equal(t, entry.lastVersion, lastVersion-1)
		}
		fs.mu.RUnlock()
	}
}

// TestJetStreamClusterSubjectVersioningRollingUpgradeRecovery models the
// rolling-upgrade scenario this fork actually cares about: a single node loses
// its on-disk svs checkpoint (sver.db) while the cluster keeps running, then
// restarts and must reconverge via the normal raft catch-up path. This is the
// closest deterministic simulation of "node was running the old binary, gets
// upgraded, must converge with the leader" without spinning two binaries in
// one Go test.
func TestJetStreamClusterSubjectVersioningRollingUpgradeRecovery(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVUPGRADE", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_UPGRADE", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	for i := 0; i < 4; i++ {
		msg := nats.NewMsg(fmt.Sprintf("events.order.123.step%d", i))
		if i == 0 {
			msg.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
		} else {
			msg.Header.Set(JSExpectedLastSubjectVer, fmt.Sprintf("%d", i-1))
		}
		ack := requestSubjectVersioningPubAck(t, nc, msg)
		require_NotNil(t, ack.SubjectVersion)
		require_Equal(t, *ack.SubjectVersion, uint64(i))
	}

	follower := c.randomNonStreamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, follower)
	followerStream, err := follower.GlobalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)
	followerFs, ok := followerStream.store.(*fileStore)
	require_True(t, ok)
	storeDir := followerFs.fcfg.StoreDir
	follower.Shutdown()

	// Drop the checkpoint while the node is down. This is what an "upgrade from
	// a binary that never wrote sver.db" would look like on disk.
	require_NoError(t, os.Remove(filepath.Join(storeDir, msgDir, subjectVersionStateFile)))

	leader := c.streamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, leader)
	leaderStream, err := leader.GlobalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)

	// More writes while the upgrading node is offline.
	for i := 4; i < 7; i++ {
		msg := nats.NewMsg(fmt.Sprintf("events.order.123.step%d", i))
		msg.Header.Set(JSExpectedLastSubjectVer, fmt.Sprintf("%d", i-1))
		ack := requestSubjectVersioningPubAck(t, nc, msg)
		require_NotNil(t, ack.SubjectVersion)
		require_Equal(t, *ack.SubjectVersion, uint64(i))
	}

	// Force a snapshot so the rejoining node has to take both the snapshot
	// install path AND the per-message header replay path during rebuild.
	err = leaderStream.raftNode().InstallSnapshot(leaderStream.stateSnapshot(), false)
	require_NoError(t, err)

	follower = c.restartServer(follower)
	c.waitOnServerHealthz(follower)
	c.waitOnStreamCurrent(follower, globalAccountName, cfg.Name)

	rejoinedStream, err := follower.GlobalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)
	rejoinedFs, ok := rejoinedStream.store.(*fileStore)
	require_True(t, ok)

	// svs must rebuild from the replicated headers and match the leader.
	rejoinedFs.mu.RLock()
	rejoinedEntry, found := rejoinedFs.svs.Find([]byte("events.order.123"))
	rejoinedFs.mu.RUnlock()
	require_True(t, found)
	require_Equal(t, rejoinedEntry.lastVersion, uint64(6))

	leaderFs, ok := leaderStream.store.(*fileStore)
	require_True(t, ok)
	leaderFs.mu.RLock()
	leaderEntry, leaderFound := leaderFs.svs.Find([]byte("events.order.123"))
	leaderFs.mu.RUnlock()
	require_True(t, leaderFound)
	require_Equal(t, rejoinedEntry.lastVersion, leaderEntry.lastVersion)
	require_Equal(t, rejoinedEntry.lastSeq, leaderEntry.lastSeq)

	// Promote the rejoined node and verify the next assignment continues from
	// the recovered svs rather than restarting the namespace.
	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: follower.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, cfg.Name), data, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	require_Equal(t, c.streamLeader(globalAccountName, cfg.Name), follower)

	probe := nats.NewMsg("events.order.123.shipped")
	probe.Header.Set(JSExpectedLastSubjectVer, "6")
	probeAck := requestSubjectVersioningPubAck(t, nc, probe)
	require_NotNil(t, probeAck.SubjectVersion)
	require_Equal(t, *probeAck.SubjectVersion, uint64(7))
	require_Equal(t, probeAck.SubjectVersionKey, "events.order.123")
}

func TestJetStreamClusterSubjectVersioningSurvivesClusterRestart(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVRESTART", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_RESTART", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	first := nats.NewMsg("events.order.123.created")
	first.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
	first.Header.Set(JSMsgId, "restart-dedupe")
	firstAck := requestSubjectVersioningPubAck(t, nc, first)
	require_NotNil(t, firstAck.SubjectVersion)
	require_Equal(t, *firstAck.SubjectVersion, uint64(0))

	second := nats.NewMsg("events.order.123.cancelled")
	second.Header.Set(JSExpectedLastSubjectVer, "0")
	secondAck := requestSubjectVersioningPubAck(t, nc, second)
	require_NotNil(t, secondAck.SubjectVersion)
	require_Equal(t, *secondAck.SubjectVersion, uint64(1))

	nc.Close()
	c.restartAllSamePorts()
	c.waitOnStreamLeader(globalAccountName, cfg.Name)

	nc, _ = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	third := nats.NewMsg("events.order.123.shipped")
	third.Header.Set(JSExpectedLastSubjectVer, "1")
	thirdAck := requestSubjectVersioningPubAck(t, nc, third)
	require_NotNil(t, thirdAck.SubjectVersion)
	require_Equal(t, *thirdAck.SubjectVersion, uint64(2))
	require_Equal(t, thirdAck.SubjectVersionKey, "events.order.123")

	duplicate := nats.NewMsg("events.order.123.created")
	duplicate.Header.Set(JSMsgId, "restart-dedupe")
	duplicateAck := requestSubjectVersioningPubAck(t, nc, duplicate)
	require_True(t, duplicateAck.Duplicate)
	require_NotNil(t, duplicateAck.SubjectVersion)
	require_Equal(t, *duplicateAck.SubjectVersion, uint64(0))
	require_Equal(t, duplicateAck.SubjectVersionKey, "events.order.123")
	require_Equal(t, duplicateAck.Sequence, firstAck.Sequence)
}

func TestJetStreamClusterSubjectVersioningFollowerCatchupFromSnapshot(t *testing.T) {
	c := createSubjectVersioningCluster(t, "SVCATCH", 3)
	defer c.shutdown()

	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_CLUSTER_CATCHUP", FileStorage)
	cfg.Replicas = 3
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	follower := c.randomNonStreamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, follower)
	follower.Shutdown()

	c.waitOnStreamLeader(globalAccountName, cfg.Name)

	for i := 0; i < 20; i++ {
		msg := nats.NewMsg(fmt.Sprintf("events.order.123.step%d", i))
		if i == 0 {
			msg.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			msg.Header.Set(JSMsgId, "catchup-dedupe")
		} else {
			msg.Header.Set(JSExpectedLastSubjectVer, fmt.Sprintf("%d", i-1))
		}
		ack := requestSubjectVersioningPubAck(t, nc, msg)
		require_NotNil(t, ack.SubjectVersion)
		require_Equal(t, *ack.SubjectVersion, uint64(i))
		require_Equal(t, ack.SubjectVersionKey, "events.order.123")
	}

	leader := c.streamLeader(globalAccountName, cfg.Name)
	require_NotNil(t, leader)
	mset, err := leader.GlobalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)
	err = mset.raftNode().InstallSnapshot(mset.stateSnapshot(), false)
	require_NoError(t, err)

	follower = c.restartServer(follower)
	c.waitOnServerHealthz(follower)
	c.waitOnStreamCurrent(follower, globalAccountName, cfg.Name)

	req := JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: follower.Name()}}
	data, err := json.Marshal(req)
	require_NoError(t, err)
	_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, cfg.Name), data, time.Second)
	require_NoError(t, err)
	c.waitOnStreamLeader(globalAccountName, cfg.Name)
	require_Equal(t, c.streamLeader(globalAccountName, cfg.Name), follower)

	next := nats.NewMsg("events.order.123.finished")
	next.Header.Set(JSExpectedLastSubjectVer, "19")
	nextAck := requestSubjectVersioningPubAck(t, nc, next)
	require_NotNil(t, nextAck.SubjectVersion)
	require_Equal(t, *nextAck.SubjectVersion, uint64(20))
	require_Equal(t, nextAck.SubjectVersionKey, "events.order.123")

	duplicate := nats.NewMsg("events.order.123.step0")
	duplicate.Header.Set(JSMsgId, "catchup-dedupe")
	duplicateAck := requestSubjectVersioningPubAck(t, nc, duplicate)
	require_True(t, duplicateAck.Duplicate)
	require_NotNil(t, duplicateAck.SubjectVersion)
	require_Equal(t, *duplicateAck.SubjectVersion, uint64(0))
	require_Equal(t, duplicateAck.SubjectVersionKey, "events.order.123")
}
