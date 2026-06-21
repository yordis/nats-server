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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamFastBatchSubjectVersioningDuplicatePubAckCarriesOriginalMetadata(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createSubjectVersioningCluster(t, "SVFASTDUP", 3)
			defer c.shutdown()

			nc, _ := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_FAST_DUP_R%d", replicas), FileStorage)
			cfg.Replicas = replicas
			cfg.AllowBatchPublish = true
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			seed := nats.NewMsg("events.order.123.created")
			seed.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			seed.Header.Set(JSMsgId, "fast-dup")
			seedAck := requestSubjectVersioningPubAck(t, nc, seed)
			require_NotNil(t, seedAck.SubjectVersion)
			require_Equal(t, *seedAck.SubjectVersion, uint64(0))

			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
			require_NoError(t, err)
			defer sub.Drain()

			dup := nats.NewMsg("events.order.123.created")
			dup.Header.Set(JSMsgId, "fast-dup")
			dup.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
			require_NoError(t, nc.PublishMsg(dup))

			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)

			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_True(t, pubAck.Error == nil)
			require_True(t, pubAck.Duplicate)
			require_Equal(t, pubAck.Sequence, seedAck.Sequence)
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 1)
			require_NotNil(t, pubAck.SubjectVersion)
			require_Equal(t, *pubAck.SubjectVersion, uint64(0))
			require_Equal(t, pubAck.SubjectVersionKey, "events.order.123")
		})
	}
}

func TestJetStreamFastBatchSubjectVersioningExpectedVersionOnlyAllowedOnFirstMessagePerNamespace(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			c := createSubjectVersioningCluster(t, "SVFASTEXP", 3)
			defer c.shutdown()

			nc, _ := jsClientConnect(t, c.randomServer())
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_FAST_EXPECT_R%d", replicas), FileStorage)
			cfg.Replicas = replicas
			cfg.AllowBatchPublish = true
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
			require_NoError(t, err)
			defer sub.Drain()

			first := nats.NewMsg("events.order.123.created")
			first.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			first.Reply = generateFastBatchReply(inbox, "uuid", 1, 2, FastBatchGapFail, FastBatchOpStart)
			require_NoError(t, nc.PublishMsg(first))

			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)

			var batchFlowAck BatchFlowAck
			require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
			require_Equal(t, batchFlowAck.Sequence, uint64(0))
			require_Equal(t, batchFlowAck.Messages, uint16(2))

			second := nats.NewMsg("events.order.123.cancelled")
			second.Header.Set(JSExpectedLastSubjectVer, "0")
			second.Reply = generateFastBatchReply(inbox, "uuid", 2, 2, FastBatchGapFail, FastBatchOpAppend)
			require_NoError(t, nc.PublishMsg(second))

			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)

			var batchFlowErr BatchFlowErr
			require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowErr))
			require_Equal(t, batchFlowErr.Sequence, uint64(2))
			require_True(t, batchFlowErr.Error != nil)
			require_Error(t, batchFlowErr.Error, NewJSStreamWrongLastSubjectVersionConstantError())

			rmsg, err = sub.NextMsg(time.Second)
			require_NoError(t, err)

			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_True(t, pubAck.Error == nil)
			require_Equal(t, pubAck.Sequence, uint64(1))
			require_Equal(t, pubAck.BatchId, "uuid")
			require_Equal(t, pubAck.BatchSize, 1)
		})
	}
}

func TestJetStreamFastBatchSubjectVersioningRejectsReservedHeaders(t *testing.T) {
	headers := []string{JSSubjectVersion, JSSubjectVersionKey}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig("SV_FAST_RESERVED_"+header, FileStorage)
			cfg.AllowBatchPublish = true
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
			require_NoError(t, err)
			defer sub.Drain()

			msg := nats.NewMsg("events.order.123.created")
			msg.Header.Set(header, "42")
			msg.Reply = generateFastBatchReply(inbox, "uuid", 1, 0, FastBatchGapFail, FastBatchOpCommit)
			require_NoError(t, nc.PublishMsg(msg))

			rmsg, err := sub.NextMsg(time.Second)
			require_NoError(t, err)

			var pubAck JSPubAckResponse
			require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
			require_True(t, pubAck.Error != nil)
			require_Error(t, pubAck.Error, NewJSStreamSubjectVersionHeaderServerManagedError(header))
		})
	}
}

func TestJetStreamFastBatchSubjectVersioningDuplicateThenVersionError(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_FAST_DUP_ERR", FileStorage)
	cfg.AllowBatchPublish = true
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	seed := nats.NewMsg("events.order.123.created")
	seed.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
	seed.Header.Set(JSMsgId, "fast-dup-err")
	requestSubjectVersioningPubAck(t, nc, seed)

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(fmt.Sprintf("%s.>", inbox))
	require_NoError(t, err)
	defer sub.Drain()

	first := nats.NewMsg("events.order.123.created")
	first.Header.Set(JSMsgId, "fast-dup-err")
	first.Reply = generateFastBatchReply(inbox, "uuid", 1, 2, FastBatchGapFail, FastBatchOpStart)
	require_NoError(t, nc.PublishMsg(first))

	rmsg, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	var batchFlowAck BatchFlowAck
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowAck))
	require_Equal(t, batchFlowAck.Messages, uint16(2))

	second := nats.NewMsg("events.order.123.cancelled")
	second.Header.Set(JSExpectedLastSubjectVer, "999")
	second.Reply = generateFastBatchReply(inbox, "uuid", 2, 2, FastBatchGapFail, FastBatchOpAppend)
	require_NoError(t, nc.PublishMsg(second))

	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)

	var batchFlowErr BatchFlowErr
	require_NoError(t, json.Unmarshal(rmsg.Data, &batchFlowErr))
	require_Equal(t, batchFlowErr.Sequence, uint64(2))
	require_True(t, batchFlowErr.Error != nil)
	require_Error(t, batchFlowErr.Error, NewJSStreamWrongLastSubjectVersionError("0"))

	rmsg, err = sub.NextMsg(time.Second)
	require_NoError(t, err)

	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(rmsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_Equal(t, pubAck.Sequence, uint64(0))
	require_Equal(t, pubAck.BatchId, "uuid")
	require_Equal(t, pubAck.BatchSize, 1)
	require_True(t, pubAck.SubjectVersion == nil)
	require_Equal(t, pubAck.SubjectVersionKey, _EMPTY_)
}
