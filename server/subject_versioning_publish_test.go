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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func requestSubjectVersioningPubAckResponse(nc *nats.Conn, msg *nats.Msg) (*JSPubAckResponse, error) {
	respMsg, err := nc.RequestMsg(msg, time.Second)
	if err != nil {
		return nil, err
	}

	var pubAck JSPubAckResponse
	if err := json.Unmarshal(respMsg.Data, &pubAck); err != nil {
		return nil, err
	}
	if pubAck.Error != nil {
		return &pubAck, pubAck.Error
	}
	if pubAck.PubAck == nil {
		return &pubAck, fmt.Errorf("missing publish acknowledgement")
	}
	return &pubAck, nil
}

func requestSubjectVersioningPubAck(t testing.TB, nc *nats.Conn, msg *nats.Msg) *JSPubAckResponse {
	t.Helper()

	pubAck, err := requestSubjectVersioningPubAckResponse(nc, msg)
	require_NoError(t, err)
	return pubAck
}

func TestJetStreamSubjectVersioningExactSubjectNamespace(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			nc, js := jsClientConnect(t, s)
			defer nc.Close()

			cfg := testSubjectVersioningStreamConfig(fmt.Sprintf("SV_EXACT_%s", storage), storage)
			cfg.Subjects = []string{"orders.*"}
			cfg.SubjectVersioning.SubjectTransform = nil
			_, err := jsStreamCreate(t, nc, &cfg)
			require_NoError(t, err)

			first := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("orders.123"))
			require_NotNil(t, first.SubjectVersion)
			require_Equal(t, *first.SubjectVersion, uint64(0))
			require_Equal(t, first.SubjectVersionKey, "orders.123")

			second := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("orders.123"))
			require_NotNil(t, second.SubjectVersion)
			require_Equal(t, *second.SubjectVersion, uint64(1))
			require_Equal(t, second.SubjectVersionKey, "orders.123")

			other := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("orders.456"))
			require_NotNil(t, other.SubjectVersion)
			require_Equal(t, *other.SubjectVersion, uint64(0))
			require_Equal(t, other.SubjectVersionKey, "orders.456")

			stored, err := js.GetMsg(cfg.Name, 2)
			require_NoError(t, err)
			require_Equal(t, stored.Header.Get(JSSubjectVersion), "1")
			require_Equal(t, stored.Header.Get(JSSubjectVersionKey), "orders.123")
		})
	}
}

func TestJetStreamSubjectVersioningGroupedNamespace(t *testing.T) {
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

func TestJetStreamSubjectVersioningExpectedLastSubjectVersionInvalidValues(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_INVALID_EXPECTED", MemoryStorage)
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	cases := []string{
		"-1",
		"1e1",
		"v1",
		"NaN",
		"0x1",
		"abc",
		"3.14",
		"18446744073709551616", // overflows uint64
	}
	for _, value := range cases {
		t.Run(fmt.Sprintf("value=%q", value), func(t *testing.T) {
			msg := nats.NewMsg("events.order.123.created")
			msg.Header.Set(JSExpectedLastSubjectVer, value)
			_, err := requestSubjectVersioningPubAckResponse(nc, msg)
			require_Error(t, err, NewJSStreamExpectedLastSubjectVersionInvalidError())
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

			stored, err := js.GetMsg(cfg.Name, firstAck.Sequence)
			require_NoError(t, err)
			require_Equal(t, stored.Header.Get(JSExpectedLastSubjectVer), _EMPTY_)
			require_Equal(t, stored.Header.Get(JSSubjectVersion), "0")
			require_Equal(t, stored.Header.Get(JSSubjectVersionKey), "events.order.123")

			second := nats.NewMsg("events.order.123.cancelled")
			second.Header.Set(JSExpectedLastSubjectVer, "0")
			secondAck := requestSubjectVersioningPubAck(t, nc, second)
			require_NotNil(t, secondAck.SubjectVersion)
			require_Equal(t, *secondAck.SubjectVersion, uint64(1))

			stored, err = js.GetMsg(cfg.Name, secondAck.Sequence)
			require_NoError(t, err)
			require_Equal(t, stored.Header.Get(JSExpectedLastSubjectVer), _EMPTY_)
			require_Equal(t, stored.Header.Get(JSSubjectVersion), "1")
			require_Equal(t, stored.Header.Get(JSSubjectVersionKey), "events.order.123")

			third := nats.NewMsg("events.order.123.shipped")
			third.Header.Set(JSExpectedLastSubjectVer, "0")
			_, err = js.PublishMsg(third)
			require_Error(t, err, NewJSStreamWrongLastSubjectVersionError("1"))

			afterRejected := nats.NewMsg("events.order.123.shipped")
			afterRejected.Header.Set(JSExpectedLastSubjectVer, "1")
			afterRejectedAck := requestSubjectVersioningPubAck(t, nc, afterRejected)
			require_NotNil(t, afterRejectedAck.SubjectVersion)
			require_Equal(t, *afterRejectedAck.SubjectVersion, uint64(2))

			missing := nats.NewMsg("events.invoice.900.issued")
			missing.Header.Set(JSExpectedLastSubjectVer, "0")
			_, err = js.PublishMsg(missing)
			require_Error(t, err, NewJSStreamWrongLastSubjectVersionError(jsExpectedLastSubjectVersionNoStream))

			afterMissingRejected := nats.NewMsg("events.invoice.900.issued")
			afterMissingRejected.Header.Set(JSExpectedLastSubjectVer, jsExpectedLastSubjectVersionNoStream)
			afterMissingRejectedAck := requestSubjectVersioningPubAck(t, nc, afterMissingRejected)
			require_NotNil(t, afterMissingRejectedAck.SubjectVersion)
			require_Equal(t, *afterMissingRejectedAck.SubjectVersion, uint64(0))
		})
	}
}

func TestJetStreamSubjectVersioningSubjectTransformFallback(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	cfg := StreamConfig{
		Name:       "SV_FALLBACK",
		Storage:    MemoryStorage,
		Subjects:   []string{"events.>"},
		Retention:  LimitsPolicy,
		MaxMsgs:    -1,
		MaxBytes:   -1,
		MaxAge:     0,
		MaxMsgsPer: -1,
		DenyDelete: true,
		DenyPurge:  true,
		SubjectVersioning: &SubjectVersioningConfig{
			Mode: SubjectVersioningModeGapless,
			SubjectTransform: &SubjectTransformConfig{
				Source:      "events.*.*.*",
				Destination: "events.$1.$2",
			},
		},
	}
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	// Matches the transform: namespace key is grouped.
	matched := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.123.created"))
	require_NotNil(t, matched.SubjectVersion)
	require_Equal(t, matched.SubjectVersionKey, "events.order.123")
	require_Equal(t, *matched.SubjectVersion, uint64(0))

	// Does not match the transform: namespace key falls back to the stored subject.
	unmatched := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.heartbeat"))
	require_NotNil(t, unmatched.SubjectVersion)
	require_Equal(t, unmatched.SubjectVersionKey, "events.heartbeat")
	require_Equal(t, *unmatched.SubjectVersion, uint64(0))

	again := requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.heartbeat"))
	require_NotNil(t, again.SubjectVersion)
	require_Equal(t, again.SubjectVersionKey, "events.heartbeat")
	require_Equal(t, *again.SubjectVersion, uint64(1))
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

func TestJetStreamSubjectVersioningStreamInfoSurfacesNamespaceCount(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_INFO", MemoryStorage)
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	info, err := js.StreamInfo(cfg.Name)
	require_NoError(t, err)
	require_True(t, info != nil)

	infoMset, err := s.GlobalAccount().lookupStream(cfg.Name)
	require_NoError(t, err)
	require_NotNil(t, infoMset.subjectVersioningInfo())
	require_Equal(t, infoMset.subjectVersioningInfo().Namespaces, 0)

	requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.123.created"))
	requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.456.created"))
	requestSubjectVersioningPubAck(t, nc, nats.NewMsg("events.order.123.cancelled"))

	require_Equal(t, infoMset.subjectVersioningInfo().Namespaces, 2)

	plainCfg := &StreamConfig{
		Name:     "PLAIN_INFO",
		Storage:  MemoryStorage,
		Subjects: []string{"plain"},
	}
	_, err = jsStreamCreate(t, nc, plainCfg)
	require_NoError(t, err)
	plainMset, err := s.GlobalAccount().lookupStream(plainCfg.Name)
	require_NoError(t, err)
	require_True(t, plainMset.subjectVersioningInfo() == nil)
}

func TestJetStreamSubjectVersioningPubAckJSONShape(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	cfg := testSubjectVersioningStreamConfig("SV_JSON_SHAPE", MemoryStorage)
	_, err := jsStreamCreate(t, nc, &cfg)
	require_NoError(t, err)

	// First publish: SubjectVersion is the zero value (0). Verify the raw JSON
	// contains the field as a number, not "null" and not omitted.
	first := nats.NewMsg("events.order.123.created")
	first.Header.Set(JSMsgId, "json-shape-dedupe")
	respMsg, err := nc.RequestMsg(first, time.Second)
	require_NoError(t, err)
	raw := string(respMsg.Data)
	require_Contains(t, raw, `"subject_version":0`)
	require_Contains(t, raw, `"subject_version_key":"events.order.123"`)
	require_True(t, !strings.Contains(raw, `"duplicate":true`))

	// Duplicate publish: subject_version[ _key] must still be present, and
	// duplicate:true must be set.
	dup := nats.NewMsg("events.order.123.created")
	dup.Header.Set(JSMsgId, "json-shape-dedupe")
	dupResp, err := nc.RequestMsg(dup, time.Second)
	require_NoError(t, err)
	dupRaw := string(dupResp.Data)
	require_Contains(t, dupRaw, `"subject_version":0`)
	require_Contains(t, dupRaw, `"subject_version_key":"events.order.123"`)
	require_Contains(t, dupRaw, `"duplicate":true`)

	// Plain (non-versioned) stream must omit subject_version[ _key].
	plainCfg := &StreamConfig{
		Name:     "PLAIN_PUBACK",
		Storage:  MemoryStorage,
		Subjects: []string{"plain"},
	}
	_, err = jsStreamCreate(t, nc, plainCfg)
	require_NoError(t, err)
	plain := nats.NewMsg("plain")
	plainResp, err := nc.RequestMsg(plain, time.Second)
	require_NoError(t, err)
	plainRaw := string(plainResp.Data)
	require_True(t, !strings.Contains(plainRaw, `"subject_version"`))
	require_True(t, !strings.Contains(plainRaw, `"subject_version_key"`))
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
