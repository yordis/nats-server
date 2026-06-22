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
	"sync/atomic"
	"testing"
)

var subjectVersioningClusterPortBase atomic.Uint32

func createSubjectVersioningCluster(t testing.TB, clusterName string, numServers int) *cluster {
	const (
		basePort = 20_000
		portStep = 32
	)

	startPort := basePort + int(subjectVersioningClusterPortBase.Add(portStep)) - portStep
	return createJetStreamCluster(t, jsClusterTempl, clusterName, _EMPTY_, numServers, startPort, true)
}
