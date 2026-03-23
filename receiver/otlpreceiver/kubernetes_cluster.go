// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver // import "go.opentelemetry.io/collector/receiver/otlpreceiver"

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

const (
	kubernetesClusterURLPath = "/kubernetes/cluster"
)

var kubernetesClusterTarget = "http://localhost:9433/kubernetes/cluster"
var kubernetesClusterClient = &http.Client{Timeout: 10 * time.Second}

func (r *otlpReceiver) handleKubernetesCluster(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handleUnmatchedMethod(resp)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	upstreamReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, kubernetesClusterTarget, bytes.NewReader(body))
	if err != nil {
		http.Error(resp, err.Error(), http.StatusInternalServerError)
		return
	}

	upstreamReq.Header = req.Header.Clone()

	upstreamResp, err := kubernetesClusterClient.Do(upstreamReq)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() {
		_ = upstreamResp.Body.Close()
	}()

	for key, values := range upstreamResp.Header {
		for _, value := range values {
			resp.Header().Add(key, value)
		}
	}

	resp.WriteHeader(upstreamResp.StatusCode)
	if _, err = io.Copy(resp, upstreamResp.Body); err != nil {
		http.Error(resp, err.Error(), http.StatusBadGateway)
	}
}
