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

func requestSubjectVersioningPubAck(t testing.TB, nc *nats.Conn, msg *nats.Msg) *JSPubAckResponse {
	t.Helper()

	respMsg, err := nc.RequestMsg(msg, time.Second)
	require_NoError(t, err)

	var pubAck JSPubAckResponse
	require_NoError(t, json.Unmarshal(respMsg.Data, &pubAck))
	require_True(t, pubAck.Error == nil)
	require_NotNil(t, pubAck.PubAck)
	return &pubAck
}

func TestJetStreamSubjectVersioningPublishesCarryCanonicalMetadata(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_PUBLISH_%s", storage), storage)
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			first := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.123.created"))
			require_NotNil(t, first.SubjectVersion)
			require_Equal(t, *first.SubjectVersion, uint64(0))
			require_Equal(t, first.SubjectVersionKey, "events.order.123")
			require_Equal(t, first.Sequence, uint64(1))

			stored, err := js.GetMsg(cfg.Name, 1)
			require_NoError(t, err)
			require_Equal(t, stored.Header.Get(JSSubjectVersion), "0")
			require_Equal(t, stored.Header.Get(JSSubjectVersionKey), "events.order.123")

			second := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.123.cancelled"))
			require_NotNil(t, second.SubjectVersion)
			require_Equal(t, *second.SubjectVersion, uint64(1))
			require_Equal(t, second.SubjectVersionKey, "events.order.123")
			require_Equal(t, second.Sequence, uint64(2))

			third := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.invoice.900.issued"))
			require_NotNil(t, third.SubjectVersion)
			require_Equal(t, *third.SubjectVersion, uint64(0))
			require_Equal(t, third.SubjectVersionKey, "events.invoice.900")
			require_Equal(t, third.Sequence, uint64(3))
		})
	}
}

func TestJetStreamSubjectVersioningExpectedLastSubjectVersion(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_EXPECT_%s", storage), storage)
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			first := nats.NewMsg("events.order.123.created")
			first.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			firstAck := requestSubjectVersioningPubAck(t, nc, first)
			require_NotNil(t, firstAck.SubjectVersion)
			require_Equal(t, *firstAck.SubjectVersion, uint64(0))

			second := nats.NewMsg("events.order.123.cancelled")
			second.Header.Set(JSExpectedLastSubjectVer, "0")
			secondAck := requestSubjectVersioningPubAck(t, nc, second)
			require_NotNil(t, secondAck.SubjectVersion)
			require_Equal(t, *secondAck.SubjectVersion, uint64(1))

			third := nats.NewMsg("events.order.123.shipped")
			third.Header.Set(JSExpectedLastSubjectVer, "0")
			_, err = js.PublishMsg(third)
			require_Error(t, err, NewJSStreamWrongLastSubjectVersionError("1"))

			missing := nats.NewMsg("events.invoice.900.issued")
			missing.Header.Set(JSExpectedLastSubjectVer, "0")
			_, err = js.PublishMsg(missing)
			require_Error(t, err, NewJSStreamWrongLastSubjectVersionError(jsExpectedLastSubjectVersionNoStream))
		})
	}
}

func TestJetStreamSubjectVersioningDuplicatePubAckReturnsOriginalMetadata(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, _ := jsClientConnect(t, s)
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_DUP_%s", storage), storage)
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			first := nats.NewMsg("events.order.123.created")
			first.Header.Set(JSMsgId, "dedupe-1")
			firstAck := requestSubjectVersioningPubAck(t, nc, first)
			require_NotNil(t, firstAck.SubjectVersion)
			require_Equal(t, *firstAck.SubjectVersion, uint64(0))
			require_False(t, firstAck.Duplicate)

			second := nats.NewMsg("events.order.123.created")
			second.Header.Set(JSMsgId, "dedupe-1")
			secondAck := requestSubjectVersioningPubAck(t, nc, second)
			require_NotNil(t, secondAck.SubjectVersion)
			require_Equal(t, *secondAck.SubjectVersion, uint64(0))
			require_Equal(t, secondAck.SubjectVersionKey, "events.order.123")
			require_Equal(t, secondAck.Sequence, firstAck.Sequence)
			require_True(t, secondAck.Duplicate)
		})
	}
}

func TestJetStreamSubjectVersioningRejectsUnsupportedHeaders(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_BAD_HEADERS", MemoryStorage)
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	legacy := nats.NewMsg("events.order.123.created")
	legacy.Header.Set(JSExpectedLastSubjSeq, "0")
	legacyAck := requestSubjectVersioningPubAck(t, nc, legacy)
	require_NotNil(t, legacyAck.SubjectVersion)
	require_Equal(t, *legacyAck.SubjectVersion, uint64(0))
	require_Equal(t, legacyAck.SubjectVersionKey, "events.order.123")

	reserved := nats.NewMsg("events.order.123.created")
	reserved.Header.Set(JSSubjectVersion, "99")
	_, err = js.PublishMsg(reserved)
	require_Error(t, err, NewJSStreamSubjectVersionHeaderServerManagedError(JSSubjectVersion))

	invalid := nats.NewMsg("events.order.123.created")
	invalid.Header.Set(JSExpectedLastSubjectVer, "abc")
	_, err = js.PublishMsg(invalid)
	require_Error(t, err, NewJSStreamExpectedLastSubjectVersionInvalidError())

	plainCfg := &StreamConfig{
		Name:     "PLAIN",
		Storage:  MemoryStorage,
		Subjects: []string{"plain"},
	}
	_, err = jsStreamCreate(t, nc, plainCfg)
	require_NoError(t, err)

	plain := nats.NewMsg("plain")
	plain.Header.Set(JSExpectedLastSubjectVer, "0")
	_, err = js.PublishMsg(plain)
	require_Error(t, err, NewJSStreamExpectedLastSubjectVersionRequiresVersioningError())
}
