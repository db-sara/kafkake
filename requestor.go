package kafkake

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/patrickmn/go-cache"
)

type KafkakeConfig struct {
	BootstrapServers string
	SecurityProtocol string
	SaslMechanisms   string
	SaslUsername     string
	SaslPassword     string
}

type CacheState int

const (
	unavailable CacheState = iota
	requested
	recieved
)

type Requestor struct {
	KeyCache      *cache.Cache
	ValueCache    *cache.Cache
	Consumer      *kafka.Consumer
	ConsumerTopic string
	Producer      *kafka.Producer
	ProducerTopic string
}

func NewRequestor(config KafkakeConfig, requestTopic, responseTopic string) (*Requestor, error) {
	c := Requestor{}

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

	keyCache := cache.New(5*time.Minute, 10*time.Minute)
	valueCache := cache.New(5*time.Minute, 10*time.Minute)

	c.KeyCache = keyCache
	c.ValueCache = valueCache

	return &c, nil
}

func (r *Requestor) Consume(duration time.Duration) {
	go func() {
		for {
			msg, err := r.Consumer.ReadMessage(duration)
			if err != nil {
				continue
			} else {
				r.KeyCache.Set(string(msg.Key), recieved, cache.DefaultExpiration)
				r.ValueCache.Set(string(msg.Key), msg, cache.DefaultExpiration)
			}
		}
	}()
}

func (r *Requestor) Request(msg *kafka.Message) {
	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range r.Producer.Events() {
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

	err := r.Producer.Produce(msg, nil)
	if err != nil {
		return
	}

	// Wait for all messages to be delivered
	r.Producer.Flush(15 * 1000)

	r.KeyCache.Set(string(msg.Key), requested, cache.DefaultExpiration)
}

func (r *Requestor) Collect(msg *kafka.Message) (*kafka.Message, error) {
	timeout := time.After(5 * time.Second)
	tick := time.Tick(500 * time.Millisecond)
	// Keep trying until we're timed out or got a result or got an error
	for {
		select {
		// Got a timeout! fail with a timeout error
		case <-timeout:
			return nil, errors.New("no response within timeout period")
		// Got a tick, we should check on doSomething()
		case <-tick:
			key := string(msg.Key)
			resp, found := r.KeyCache.Get(key)
			if found {
				if resp == recieved {
					for i := 0; i < 10; i++ {
						response, found := r.ValueCache.Get(key)

						if found {
							respMsg, ok := response.(*kafka.Message)
							if ok {
								return respMsg, nil
							}
						}
					}
				}
			} else {
				return nil, errors.New("no request ever made")
			}
		}
	}
	return nil, errors.New("no response made successfully")
}
