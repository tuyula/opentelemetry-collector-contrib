// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"github.com/Shopify/sarama"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type pdataLogsMarshaler struct {
	marshaler plog.Marshaler
	encoding  string
}

func (p pdataLogsMarshaler) Marshal(ld plog.Logs, topic string) ([]*sarama.ProducerMessage, error) {
	bts, err := p.marshaler.MarshalLogs(ld)
	if err != nil {
		return nil, err
	}
	return []*sarama.ProducerMessage{
		{
			Topic: topic,
			Value: sarama.ByteEncoder(bts),
		},
	}, nil
}

func (p pdataLogsMarshaler) Encoding() string {
	return p.encoding
}

func newPdataLogsMarshaler(marshaler plog.Marshaler, encoding string) LogsMarshaler {
	return pdataLogsMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}

type pdataMetricsMarshaler struct {
	marshaler pmetric.Marshaler
	encoding  string
}

func (p pdataMetricsMarshaler) Marshal(ld pmetric.Metrics, topic string) ([]*sarama.ProducerMessage, error) {
	bts, err := p.marshaler.MarshalMetrics(ld)
	if err != nil {
		return nil, err
	}
	return []*sarama.ProducerMessage{
		{
			Topic: topic,
			Value: sarama.ByteEncoder(bts),
		},
	}, nil
}

func (p pdataMetricsMarshaler) Encoding() string {
	return p.encoding
}

func newPdataMetricsMarshaler(marshaler pmetric.Marshaler, encoding string) MetricsMarshaler {
	return pdataMetricsMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}

type pdataTracesMarshaler struct {
	marshaler ptrace.Marshaler
	encoding  string
}

func (p pdataTracesMarshaler) Marshal(td ptrace.Traces, topic string) ([]*sarama.ProducerMessage, error) {
	idToTrace := p.groupResourceSpansByTraceKey(td)
	var producerMessages []*sarama.ProducerMessage

	for traceId, traceSlices := range idToTrace {
		traceId16Bytes := traceId.Bytes()
		traceIdBytes := traceId16Bytes[:]
		for _, trace := range traceSlices {
			bts, err := p.marshaler.MarshalTraces(trace)
			if err != nil {
				return nil, err
			}
			producerMessages = append(producerMessages,
				&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.ByteEncoder(traceIdBytes),
					Value: sarama.ByteEncoder(bts),
				})
		}
	}

	return producerMessages, nil
}

func (p pdataTracesMarshaler) Encoding() string {
	return p.encoding
}

func (p *pdataTracesMarshaler) groupResourceSpansByTraceKey(td ptrace.Traces) map[pcommon.TraceID][]ptrace.Traces {
	idToTrace := make(map[pcommon.TraceID][]ptrace.Traces)
	rsss := td.ResourceSpans()
	for j := 0; j < rsss.Len(); j++ {
		resourceSpans := rsss.At(j)
		idToResourceSpans := p.groupSpansByTraceKey(resourceSpans)
		for traceId, rs := range idToResourceSpans {
			nTrace := ptrace.NewTraces()
			nrs := nTrace.ResourceSpans().AppendEmpty()
			rs.CopyTo(nrs)
			idToTrace[traceId] = append(idToTrace[traceId], nTrace)
		}
	}
	return idToTrace
}

func (p *pdataTracesMarshaler) groupSpansByTraceKey(resourceSpans ptrace.ResourceSpans) map[pcommon.TraceID]*ptrace.ResourceSpans {
	idToResourceSpans := make(map[pcommon.TraceID]*ptrace.ResourceSpans)
	ilss := resourceSpans.ScopeSpans()
	for j := 0; j < ilss.Len(); j++ {
		spans := ilss.At(j).Spans()
		spansLen := spans.Len()
		for k := 0; k < spansLen; k++ {
			span := spans.At(k)
			key := span.TraceID()
			nSpan := idToResourceSpans[key].ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			span.MoveTo(nSpan)
		}
	}
	return idToResourceSpans
}

func newPdataTracesMarshaler(marshaler ptrace.Marshaler, encoding string) TracesMarshaler {
	return pdataTracesMarshaler{
		marshaler: marshaler,
		encoding:  encoding,
	}
}
