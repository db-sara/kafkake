package kafkake

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Handler is a function that takes a specified object and creates a response
type Handler func([]byte) ([]byte, error)

// Exchanger represents a request-response exchanger, handling requests and responses with the specified schemas
type Exchanger struct {
	Client         *Client
	Handler        Handler
	RequestSchema  interface{}
	ResponseSchema interface{}
}

// NewExchanger creates a new message exchanger for a client with a custom handler for messages
func NewExchanger(c *Client, h Handler, reqSchema, respSchema interface{}) *Exchanger {
	return &Exchanger{
		Client:         c,
		Handler:        h,
		RequestSchema:  reqSchema,
		ResponseSchema: respSchema,
	}
}

// Handle produces a kafka response message from a kafka request message
func (e *Exchanger) Handle(req *kafka.Message) (*kafka.Message, error) {
	body, err := e.Handler(req.Value)

	if err != nil {
		return nil, err
	}

	resp := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &e.Client.ProducerTopic,
			Partition: kafka.PartitionAny,
		},
		Value:         body,
		Key:           req.Key,
		Timestamp:     time.Now(),
		TimestampType: kafka.TimestampCreateTime,
	}
	return &resp, nil
}
