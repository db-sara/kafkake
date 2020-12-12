package kafkake

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// State represents the current state of the Kafkake client
type State int

const (
	notReady State = iota // notReady indicates the client has not been initialized
	online                // online indicates that the client is ready for communication
	offline               // offline indicates that the client has been shut down
)

// ClientOption defines possible options that can be used to customize the client
type ClientOption func(client *Client)

// ClientConfig defines the parameters to be used by the client
type ClientConfig struct {
	BootstrapServers string
	SecurityProtocol string
	SaslMechanisms   string
	SaslUsername     string
	SaslPassword     string
	RequestTopic     string
	ResponseTopic    string
}

// Client is the interface to interact with a kafka cluster
type Client struct {
	State         State
	Exchanger     *Exchanger
	Consumer      *kafka.Consumer
	ConsumerTopic string
	Producer      *kafka.Producer
	ProducerTopic string
}

// WithExchanger customizes the client to use a specified Exchanger
func WithExchanger(e *Exchanger) ClientOption {
	return func(c *Client) {
		c.Exchanger = e
	}
}

// WithNewExchanger customizes the client to use a newly specified Exchanger
func WithNewExchanger(h Handler, reqSchema, respSchema interface{}) ClientOption {
	return func(c *Client) {
		c.Exchanger = NewExchanger(c, h, reqSchema, respSchema)
	}
}

// NewClient creates a new client object with the specified configuration and options
func NewClient(config ClientConfig, requestTopic, responseTopic string, opts ...ClientOption) (*Client, error) {
	c := Client{State: notReady}

	// Create Consumer instance
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"sasl.mechanisms":   config.SaslMechanisms,
		"security.protocol": config.SecurityProtocol,
		"sasl.username":     config.SaslUsername,
		"sasl.password":     config.SaslPassword,
		"group.id":          "go_" + strings.ReplaceAll(requestTopic, "-", "_") + "_group",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	// Subscribe to topic
	err = consumer.SubscribeTopics([]string{requestTopic}, nil)

	if err != nil {
		return nil, err
	}

	c.Consumer = consumer
	c.ConsumerTopic = requestTopic

	// Create Producer instance
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"sasl.mechanisms":   config.SaslMechanisms,
		"security.protocol": config.SecurityProtocol,
		"sasl.username":     config.SaslUsername,
		"sasl.password":     config.SaslPassword})

	if err != nil {
		return nil, err
	}

	c.Producer = producer
	c.ProducerTopic = responseTopic

	c.State = online

	// Loop through each option
	for _, opt := range opts {
		opt(&c)
	}

	return &c, nil
}

// ReadMessage waits for a message from the Kafka cluster to be received on the current request topic
func (c *Client) ReadMessage(duration time.Duration) (*kafka.Message, error) {
	if c.State != online {
		return nil, fmt.Errorf("client is not online")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var err error
	run := true
	for run {
		select {
		case sig := <-sigChan:
			err = fmt.Errorf("caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.Consumer.ReadMessage(duration)
			if err != nil {
				continue
			} else {
				return msg, nil
			}
		}
	}
	return nil, err
}

// SendMessage sends a message from the Kafka cluster to be received on the current request topic
func (c *Client) SendMessage(msg *kafka.Message) error {
	if c.State != online {
		return fmt.Errorf("client is not online")
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range c.Producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	err := c.Producer.Produce(msg, nil)
	if err != nil {
		return err
	}

	// Wait for all messages to be delivered
	c.Producer.Flush(15 * 1000)

	return nil
}

// ExchangeMessage waits for a message and sends a message to the kafka cluster with the parameterized Exchanger
func (c *Client) ExchangeMessage() error {
	if c.State != online {
		return fmt.Errorf("client is not online")
	} else if c.Exchanger == nil {
		return fmt.Errorf("exchanger not available")
	}

	req, err := c.ReadMessage(100 * time.Second)
	if err != nil {
		return err
	}
	resp, err := c.Exchanger.Handle(req)
	if err != nil {
		return err
	}
	return c.SendMessage(resp)
}

// Shutdown safely closes the client
func (c *Client) Shutdown() error {
	if c.State != online {
		return fmt.Errorf("client is not online")
	}
	c.State = offline
	c.Producer.Close()
	err := c.Consumer.Close()
	return err
}

// ExchangeLoop starts a loop to indefinitely wait and handle messages on the Kafka cluster
func (c *Client) ExchangeLoop() error {
	// Set up a channel for handling Ctrl-C, etc
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	run := true
	for run == true {
		select {
		case sig := <-sigChan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			err := c.ExchangeMessage()
			if err != nil {
				return err
			}
		}
	}

	return c.Shutdown()
}
