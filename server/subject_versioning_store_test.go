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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testSubjectVersioningStreamConfig(name string, storage StorageType) StreamConfig {
	return StreamConfig{
		Name:       name,
		Storage:    storage,
		Subjects:   []string{"events.*.*.*"},
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
}

func BenchmarkSubjectVersioningHighCardinalityStore(b *testing.B) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		b.Run(storage.String(), func(b *testing.B) {
			cfg := testSubjectVersioningStreamConfig("SV_BENCH_"+storage.String(), storage)
			var store StreamStore
			switch storage {
			case MemoryStorage:
				ms, err := newMemStore(&cfg)
				require_NoError(b, err)
				store = ms
			case FileStorage:
				fs, err := newFileStore(FileStoreConfig{StoreDir: b.TempDir()}, cfg)
				require_NoError(b, err)
				store = fs
			}
			b.Cleanup(func() {
				require_NoError(b, store.Stop())
			})

			payload := []byte("payload")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("events.order.%d", i)
				hdr := genHeader(nil, JSSubjectVersion, "0")
				hdr = genHeader(hdr, JSSubjectVersionKey, key)
				_, _, err := store.StoreMsg(key+".created", hdr, payload, 0)
				require_NoError(b, err)
			}
			b.StopTimer()

			switch store := store.(type) {
			case *memStore:
				store.mu.RLock()
				require_Equal(b, store.svs.Size(), b.N)
				store.mu.RUnlock()
			case *fileStore:
				store.mu.RLock()
				require_Equal(b, store.svs.Size(), b.N)
				store.mu.RUnlock()
			}
		})
	}
}

func BenchmarkSubjectVersioningSustainedPublish(b *testing.B) {
	const namespaces = 100_000

	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		b.Run(storage.String(), func(b *testing.B) {
			cfg := testSubjectVersioningStreamConfig("SV_SOAK_"+storage.String(), storage)
			var store StreamStore
			switch storage {
			case MemoryStorage:
				ms, err := newMemStore(&cfg)
				require_NoError(b, err)
				store = ms
			case FileStorage:
				fs, err := newFileStore(FileStoreConfig{StoreDir: b.TempDir()}, cfg)
				require_NoError(b, err)
				store = fs
			}
			b.Cleanup(func() {
				require_NoError(b, store.Stop())
			})

			payload := []byte("payload")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ns := i % namespaces
				key := fmt.Sprintf("events.order.%d", ns)
				hdr := genHeader(nil, JSSubjectVersion, fmt.Sprintf("%d", i/namespaces))
				hdr = genHeader(hdr, JSSubjectVersionKey, key)
				_, _, err := store.StoreMsg(key+".event", hdr, payload, 0)
				require_NoError(b, err)
			}
			b.StopTimer()

			switch store := store.(type) {
			case *memStore:
				store.mu.RLock()
				b.ReportMetric(float64(store.svs.Size()), "namespaces")
				store.mu.RUnlock()
			case *fileStore:
				store.mu.RLock()
				b.ReportMetric(float64(store.svs.Size()), "namespaces")
				store.mu.RUnlock()
			}
		})
	}
}

func BenchmarkFileStoreSubjectVersionStateCheckpoint(b *testing.B) {
	const namespaces = 10_000

	cfg := testSubjectVersioningStreamConfig("SV_BENCH_CHECKPOINT", FileStorage)
	fs, err := newFileStore(FileStoreConfig{StoreDir: b.TempDir()}, cfg)
	require_NoError(b, err)
	b.Cleanup(func() {
		require_NoError(b, fs.Stop())
	})

	payload := []byte("payload")
	for i := 0; i < namespaces; i++ {
		key := fmt.Sprintf("events.order.%d", i)
		hdr := genHeader(nil, JSSubjectVersion, "0")
		hdr = genHeader(hdr, JSSubjectVersionKey, key)
		_, _, err := fs.StoreMsg(key+".created", hdr, payload, 0)
		require_NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require_NoError(b, fs.writeSubjectVersionState())
	}
}

func TestJetStreamSubjectVersioningConfigValidation(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	tests := []struct {
		name   string
		mutate func(*StreamConfig)
		want   string
	}{
		{
			name: "max-age",
			mutate: func(cfg *StreamConfig) {
				cfg.MaxAge = time.Minute
			},
			want: "subject versioning requires max age to be disabled",
		},
		{
			name: "message-ttl",
			mutate: func(cfg *StreamConfig) {
				cfg.AllowMsgTTL = true
			},
			want: "subject versioning does not allow message TTLs",
		},
		{
			name: "message-schedules",
			mutate: func(cfg *StreamConfig) {
				cfg.AllowMsgSchedules = true
			},
			want: "subject versioning requires deny purge",
		},
		{
			name: "mirror",
			mutate: func(cfg *StreamConfig) {
				cfg.Mirror = &StreamSource{Name: "ORIGIN"}
			},
			want: "subject versioning does not support mirrors",
		},
		{
			name: "source",
			mutate: func(cfg *StreamConfig) {
				cfg.Sources = []*StreamSource{{Name: "ORIGIN"}}
			},
			want: "subject versioning does not support sources",
		},
		{
			name: "republish",
			mutate: func(cfg *StreamConfig) {
				cfg.RePublish = &RePublish{Destination: "copy.>"}
			},
			want: "subject versioning does not support republish",
		},
		{
			name: "bad-transform",
			mutate: func(cfg *StreamConfig) {
				cfg.SubjectVersioning.SubjectTransform = &SubjectTransformConfig{
					Source:      "events.*.*.*",
					Destination: "events.$1.$9",
				}
			},
			want: "subject versioning transform destination is invalid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testSubjectVersioningStreamConfig("SV_"+tc.name, MemoryStorage)
			tc.mutate(&cfg)
			_, err := s.GlobalAccount().addStream(&cfg)
			require_Error(t, err)
			require_Contains(t, err.Error(), tc.want)
		})
	}
}

func TestJetStreamSubjectVersioningUpdateRequiresEmptyStream(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "SV_UPDATE",
		Storage:  MemoryStorage,
		Subjects: []string{"events.order.*.*"},
	})
	require_NoError(t, err)

	_, _, err = mset.store.StoreMsg("events.order.123.created", nil, []byte("one"), 0)
	require_NoError(t, err)

	cfg := mset.cfg.clone()
	cfg.DenyDelete = true
	cfg.DenyPurge = true
	cfg.SubjectVersioning = &SubjectVersioningConfig{
		Mode: SubjectVersioningModeGapless,
		SubjectTransform: &SubjectTransformConfig{
			Source:      "events.order.*.*",
			Destination: "events.order.$1",
		},
	}
	err = mset.update(cfg)
	require_Error(t, err)
	require_Contains(t, err.Error(), "subject versioning can only be changed on an empty stream")
}

func TestJetStreamSubjectVersioningUpdateRejectsDisallowedCombos(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*StreamConfig)
		want   string
	}{
		{
			name: "max-age",
			mutate: func(cfg *StreamConfig) {
				cfg.MaxAge = time.Hour
				cfg.Duplicates = time.Minute
			},
			want: "subject versioning requires max age to be disabled",
		},
		{
			name: "max-msgs",
			mutate: func(cfg *StreamConfig) {
				cfg.MaxMsgs = 1
			},
			want: "subject versioning requires max messages to be disabled",
		},
		{
			name: "max-bytes",
			mutate: func(cfg *StreamConfig) {
				cfg.MaxBytes = 1
			},
			want: "subject versioning requires max bytes to be disabled",
		},
		{
			name: "max-msgs-per",
			mutate: func(cfg *StreamConfig) {
				cfg.MaxMsgsPer = 1
			},
			want: "subject versioning requires max messages per subject to be disabled",
		},
		{
			name: "allow-msg-ttl",
			mutate: func(cfg *StreamConfig) {
				cfg.AllowMsgTTL = true
			},
			want: "subject versioning does not allow message TTLs",
		},
		{
			name: "mirror",
			mutate: func(cfg *StreamConfig) {
				cfg.Mirror = &StreamSource{Name: "ORIGIN"}
			},
			want: "subject versioning does not support mirrors",
		},
		{
			name: "sources",
			mutate: func(cfg *StreamConfig) {
				cfg.Sources = []*StreamSource{{Name: "ORIGIN"}}
			},
			want: "subject versioning does not support sources",
		},
		{
			name: "republish",
			mutate: func(cfg *StreamConfig) {
				cfg.RePublish = &RePublish{Destination: "copy.>"}
			},
			want: "subject versioning does not support republish",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()

			cfg := testSubjectVersioningStreamConfig("SV_UPDATE_"+tc.name, MemoryStorage)
			mset, err := s.GlobalAccount().addStream(&cfg)
			require_NoError(t, err)

			updated := mset.cfg.clone()
			tc.mutate(updated)
			err = mset.update(updated)
			require_Error(t, err)
			require_Contains(t, err.Error(), tc.want)
		})
	}
}

func TestMemStoreSubjectVersionStateTracksCanonicalHeaders(t *testing.T) {
	cfg := testSubjectVersioningStreamConfig("SV_MEM", MemoryStorage)
	ms, err := newMemStore(&cfg)
	require_NoError(t, err)
	defer ms.Stop()

	hdr := genHeader(nil, JSSubjectVersion, "0")
	hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
	_, _, err = ms.StoreMsg("events.order.123.created", hdr, []byte("one"), 0)
	require_NoError(t, err)

	hdr = genHeader(nil, JSSubjectVersion, "1")
	hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
	_, _, err = ms.StoreMsg("events.order.123.cancelled", hdr, []byte("two"), 0)
	require_NoError(t, err)

	ms.mu.RLock()
	sv, ok := ms.svs.Find([]byte("events.order.123"))
	require_True(t, ok)
	require_Equal(t, sv.lastVersion, uint64(1))
	require_Equal(t, sv.lastSeq, uint64(2))
	ms.mu.RUnlock()

	require_NoError(t, ms.Truncate(1))

	ms.mu.RLock()
	sv, ok = ms.svs.Find([]byte("events.order.123"))
	require_True(t, ok)
	require_Equal(t, sv.lastVersion, uint64(0))
	require_Equal(t, sv.lastSeq, uint64(1))
	ms.mu.RUnlock()

	require_NoError(t, ms.reset())
	ms.mu.RLock()
	require_Equal(t, ms.svs.Size(), 0)
	ms.mu.RUnlock()
}

func TestFileStoreSubjectVersionStateRebuildsAfterCheckpointDeleted(t *testing.T) {
	cfg := testSubjectVersioningStreamConfig("SV_FILE_REBUILD", FileStorage)
	dir := t.TempDir()

	fs, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)

	for i := 0; i < 3; i++ {
		hdr := genHeader(nil, JSSubjectVersion, fmt.Sprintf("%d", i))
		hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
		_, _, err = fs.StoreMsg("events.order.123.step", hdr, []byte("payload"), 0)
		require_NoError(t, err)
	}
	require_NoError(t, fs.forceWriteFullState())
	require_NoError(t, fs.Stop())

	// Delete the checkpoint to force a linear rebuild from message headers.
	require_NoError(t, os.Remove(filepath.Join(dir, msgDir, subjectVersionStateFile)))

	reopened, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)
	defer reopened.Stop()

	reopened.mu.RLock()
	sv, ok := reopened.svs.Find([]byte("events.order.123"))
	require_True(t, ok)
	require_Equal(t, sv.lastVersion, uint64(2))
	require_Equal(t, sv.lastSeq, uint64(3))
	reopened.mu.RUnlock()
}

func TestFileStoreSubjectVersionStateRebuildsAfterCheckpointAheadOfStream(t *testing.T) {
	cfg := testSubjectVersioningStreamConfig("SV_FILE_AHEAD", FileStorage)
	dir := t.TempDir()

	fs, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)

	hdr := genHeader(nil, JSSubjectVersion, "0")
	hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
	_, _, err = fs.StoreMsg("events.order.123.created", hdr, []byte("one"), 0)
	require_NoError(t, err)
	require_NoError(t, fs.forceWriteFullState())

	// Hand-craft a checkpoint whose claimed sequence is far ahead of stream state.
	fs.mu.Lock()
	fs.state.LastSeq = 1
	fs.mu.Unlock()
	bogus := []byte{subjectVersionMagic, subjectVersionVer}
	bogus = append(bogus, 0xff, 0xff, 0xff, 0x7f) // checkpointSeq well beyond LastSeq+1
	bogus = append(bogus, 0)                      // numEntries = 0
	require_NoError(t, os.WriteFile(filepath.Join(dir, msgDir, subjectVersionStateFile), bogus, 0o644))
	require_NoError(t, fs.Stop())

	reopened, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)
	defer reopened.Stop()

	reopened.mu.RLock()
	sv, ok := reopened.svs.Find([]byte("events.order.123"))
	require_True(t, ok)
	require_Equal(t, sv.lastVersion, uint64(0))
	require_Equal(t, sv.lastSeq, uint64(1))
	reopened.mu.RUnlock()
}

func TestFileStoreSubjectVersionStateRecoversFromCheckpoint(t *testing.T) {
	cfg := testSubjectVersioningStreamConfig("SV_FILE", FileStorage)
	dir := t.TempDir()

	fs, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)

	hdr := genHeader(nil, JSSubjectVersion, "0")
	hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
	_, _, err = fs.StoreMsg("events.order.123.created", hdr, []byte("one"), 0)
	require_NoError(t, err)

	hdr = genHeader(nil, JSSubjectVersion, "1")
	hdr = genHeader(hdr, JSSubjectVersionKey, "events.order.123")
	_, _, err = fs.StoreMsg("events.order.123.cancelled", hdr, []byte("two"), 0)
	require_NoError(t, err)

	require_NoError(t, fs.forceWriteFullState())
	_, err = os.Stat(filepath.Join(dir, msgDir, subjectVersionStateFile))
	require_NoError(t, err)
	require_NoError(t, fs.Stop())

	reopened, err := newFileStore(FileStoreConfig{StoreDir: dir}, cfg)
	require_NoError(t, err)
	defer reopened.Stop()

	reopened.mu.RLock()
	sv, ok := reopened.svs.Find([]byte("events.order.123"))
	require_True(t, ok)
	require_Equal(t, sv.lastVersion, uint64(1))
	require_Equal(t, sv.lastSeq, uint64(2))
	reopened.mu.RUnlock()
}
