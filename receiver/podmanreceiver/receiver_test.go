// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows
// +build !windows

// nolint:errcheck
package podmanreceiver

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
	"go.uber.org/zap"
)

func TestNewReceiver(t *testing.T) {
	config := &Config{
		Endpoint: "unix:///run/some.sock",
		ScraperControllerSettings: scraperhelper.ScraperControllerSettings{
			CollectionInterval: 1 * time.Second,
		},
	}
	nextConsumer := consumertest.NewNop()
	mr, err := newReceiver(context.Background(), componenttest.NewNopReceiverCreateSettings(), config, nextConsumer, nil)

	assert.NotNil(t, mr)
	assert.Nil(t, err)
}

func TestNewReceiverErrors(t *testing.T) {
	r, err := newReceiver(context.Background(), componenttest.NewNopReceiverCreateSettings(), &Config{}, consumertest.NewNop(), nil)
	assert.Nil(t, r)
	require.Error(t, err)
	assert.Equal(t, "config.Endpoint must be specified", err.Error())

	r, err = newReceiver(context.Background(), componenttest.NewNopReceiverCreateSettings(), &Config{Endpoint: "someEndpoint"}, consumertest.NewNop(), nil)
	assert.Nil(t, r)
	require.Error(t, err)
	assert.Equal(t, "config.CollectionInterval must be specified", err.Error())
}

func TestScraperLoop(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.CollectionInterval = 100 * time.Millisecond

	client := make(mockClient)
	consumer := make(mockConsumer)

	r, err := newReceiver(context.Background(), componenttest.NewNopReceiverCreateSettings(), cfg, consumer, client.factory)
	require.NoError(t, err)
	assert.NotNil(t, r)

	go func() {
		client <- containerStatsReport{
			Stats: []containerStats{{
				ContainerID: "c1",
			}},
			Error: containerStatsReportError{},
		}
	}()

	r.Start(context.Background(), componenttest.NewNopHost())

	md := <-consumer
	assert.Equal(t, md.ResourceMetrics().Len(), 1)

	r.Shutdown(context.Background())
}

type mockClient chan containerStatsReport

func (c mockClient) factory(logger *zap.Logger, cfg *Config) (PodmanClient, error) {
	return c, nil
}

func (c mockClient) stats(context.Context, url.Values) ([]containerStats, error) {
	report := <-c
	if report.Error.Message != "" {
		return nil, errors.New(report.Error.Message)
	}
	return report.Stats, nil
}

func (c mockClient) ping(context.Context) error {
	return nil
}

type mockConsumer chan pmetric.Metrics

func (c mockClient) list(context.Context, url.Values) ([]container, error) {
	return []container{{ID: "c1"}}, nil
}

func (c mockClient) events(context.Context, url.Values) (<-chan event, <-chan error) {
	return nil, nil
}

func (m mockConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (m mockConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	m <- md
	return nil
}
