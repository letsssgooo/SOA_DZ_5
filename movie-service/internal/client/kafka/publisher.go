package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	eventsv1 "github.com/letsssgooo/SOA_DZ_5/gen/go/cinema/events/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	MovieEventsTopic       = "movie-events"
	confluentVersion  byte = 0
	firstMessageIndex byte = 0

	publishTimeout         = 8 * time.Second
	brokerDialTimeout      = 2 * time.Second
	produceRequestTimeout  = 2 * time.Second
	requestTimeoutOverhead = 2 * time.Second
	recordRetries          = 3
)

type Publisher struct {
	client   *kgo.Client
	schemaID uint32
	logger   *slog.Logger
}

func NewPublisher(brokers []string, schemaID uint32, logger *slog.Logger) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("movie-service"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RecordRetries(recordRetries),
		kgo.RecordDeliveryTimeout(publishTimeout),
		kgo.DialTimeout(brokerDialTimeout),
		kgo.ProduceRequestTimeout(produceRequestTimeout),
		kgo.RequestTimeoutOverhead(requestTimeoutOverhead),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &Publisher{client: client, schemaID: schemaID, logger: logger}, nil
}

func (p *Publisher) Close() {
	p.client.Close()
}

func (p *Publisher) Publish(ctx context.Context, event *eventsv1.MovieEvent) error {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	prefix := binary.BigEndian.AppendUint32([]byte{confluentVersion}, p.schemaID)
	prefix = append(prefix, firstMessageIndex)
	value, err := proto.MarshalOptions{}.MarshalAppend(prefix, event)
	if err != nil {
		return fmt.Errorf("encode movie event: %w", err)
	}

	eventID := event.GetEventId()
	eventType := event.GetEventType().String()
	timestamp := time.UnixMilli(event.GetTimestamp()).UTC()
	record := &kgo.Record{Topic: MovieEventsTopic, Key: []byte(event.GetUserId()), Value: value}
	ack := make(chan error, 1)
	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err == nil {
			p.logger.Info("event published", "event_id", eventID, "event_type", eventType, "timestamp", timestamp)
		}
		ack <- err
	})

	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
