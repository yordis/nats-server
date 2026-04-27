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

package server

import (
	"encoding/json"
	"fmt"
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
