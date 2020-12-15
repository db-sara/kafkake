package kafkake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

type Handler func([]byte) ([]byte, error)

type ProcessorConfig struct {
	BootstrapServers string
	SecurityProtocol string
	SaslMechanisms   string
	SaslUsername     string
	SaslPassword     string
}

type Processor struct {
	ProcessorConfig
	State         State
	RequestTopic  string
	ResponseTopic string
	Consumer      *kafka.Consumer
	Producers     map[int32]*kafka.Producer
	Handler       Handler
	MessageStates map[string]*MessageState
}

// MessageState
type MessageStatus int

const (
	ready MessageStatus = iota
	delivered
	processed
	errored
	done
)

type MessageState struct {
	key       string // Name of intersection
	req       []byte
	resp      []byte
	partition int32 // Input partition
	status    MessageStatus

	stopchan    chan struct{}
	stoppedchan chan struct{}
	lastChange  time.Time // Time of last light change
}

// groupRebalance is triggered on consumer rebalance.
//
// For each assigned partition a transactional producer is created, this is
// required to guarantee per-partition offset commit state prior to
// KIP-447 being supported.
func (p *Processor) groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {
	glog.Infof("Processor: rebalance event %v", event)

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// Create a producer per input partition.
		for _, tp := range e.Partitions {
			err := p.createTransactionalProducer(tp)
			if err != nil {
				glog.Fatal(err)
			}
		}

		err := consumer.Assign(e.Partitions)
		if err != nil {
			glog.Fatal(err)
		}

	case kafka.RevokedPartitions:
		// Abort any current transactions and close the
		// per-partition producers.
		for _, producer := range p.Producers {
			err := p.destroyTransactionalProducer(producer)
			if err != nil {
				glog.Fatal(err)
			}
		}

		// Clear producer and intersection states
		p.Producers = make(map[int32]*kafka.Producer)
		p.MessageStates = make(map[string]*MessageState)

		err := consumer.Unassign()
		if err != nil {
			glog.Fatal(err)
		}
	}

	return nil
}

func NewProcessor(config ProcessorConfig, requestTopic, responseTopic string, handler Handler) (*Processor, error) {
	p := Processor{ProcessorConfig: config}

	// The per-partition producers are set up in groupRebalance
	p.Producers = make(map[int32]*kafka.Producer)

	consumerConfig := &kafka.ConfigMap{
		"client.id":         fmt.Sprintf("go-%s-processor", requestTopic),
		"bootstrap.servers": config.BootstrapServers,
		"sasl.mechanisms":   config.SaslMechanisms,
		"security.protocol": config.SecurityProtocol,
		"sasl.username":     config.SaslUsername,
		"sasl.password":     config.SaslPassword,
		"group.id":          fmt.Sprintf("go-%s-group", requestTopic),
		"auto.offset.reset": "earliest",
		// Consumer used for input to a transactional processor
		// must have auto-commits disabled since offsets
		// are committed with the transaction using
		// SendOffsetsToTransaction.
		"enable.auto.commit": false,
	}

	var err error
	p.Consumer, err = kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, err
	}

	err = p.Consumer.Subscribe(requestTopic, p.groupRebalance)
	if err != nil {
		return nil, err
	}

	p.Handler = handler

	p.RequestTopic = requestTopic
	p.ResponseTopic = responseTopic

	p.MessageStates = make(map[string]*MessageState)

	p.State = online

	return &p, nil
}

// createTransactionalProducer creates a transactional producer for the given
// input partition.
func (p *Processor) createTransactionalProducer(toppar kafka.TopicPartition) error {
	producerConfig := &kafka.ConfigMap{
		"client.id":         fmt.Sprintf("go-%s-producer-%d", p.RequestTopic, int(toppar.Partition)),
		"bootstrap.servers": p.BootstrapServers,
		"sasl.mechanisms":   p.SaslMechanisms,
		"security.protocol": p.SecurityProtocol,
		"sasl.username":     p.SaslUsername,
		"sasl.password":     p.SaslPassword,
		"transactional.id":  fmt.Sprintf("go-%s-transactions-producer-%d", p.RequestTopic, int(toppar.Partition)),
	}

	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return err
	}

	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.InitTransactions(ctx)
	if err != nil {
		return err
	}

	err = producer.BeginTransaction()
	if err != nil {
		return err
	}

	p.Producers[toppar.Partition] = producer
	glog.Infof("Processor: created producer %s for partition %v",
		p.Producers[toppar.Partition], toppar.Partition)
	return nil
}

// destroyTransactionalProducer aborts the current transaction and destroys the producer.
func (p *Processor) destroyTransactionalProducer(producer *kafka.Producer) error {
	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.AbortTransaction(ctx)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			glog.Errorf("Failed to abort transaction for %s: %s",
				producer, err)
		}
	}

	producer.Close()

	return err
}

// getConsumerPosition gets the current position (next offset) for a given input partition.
func (p *Processor) getConsumerPosition(partition int32) []kafka.TopicPartition {
	position, err := p.Consumer.Position([]kafka.TopicPartition{{Topic: &p.RequestTopic, Partition: partition}})
	if err != nil {
		glog.Fatal(err)
	}

	return position
}

// rewindConsumerPosition rewinds the consumer to the last committed offset or
// the beginning of the partition if there is no committed offset.
// This is to be used when the current transaction is aborted.
func (p *Processor) rewindConsumerPosition(partition int32) {
	committed, err := p.Consumer.Committed([]kafka.TopicPartition{{Topic: &p.RequestTopic, Partition: partition}}, 10*1000 /* 10s */)
	if err != nil {
		glog.Fatal(err)
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
		}

		glog.Infof("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset)

		err = p.Consumer.Seek(tp, -1)
		if err != nil {
			glog.Fatal(err)
		}
	}
}

func (p *Processor) getMessageState(key string, req []byte, partition int32) *MessageState {
	msgState, found := p.MessageStates[key]
	if found {
		return msgState
	}

	msgState = &MessageState{
		key:        key,
		req:        req,
		partition:  partition,
		lastChange: time.Now(),
		status:     ready,
	}

	p.MessageStates[key] = msgState

	return msgState
}

func (p *Processor) processIngressMessage(msg *kafka.Message) {
	if msg.Value == nil {
		// Invalid message, ignore
		return
	}

	if msg.Key == nil {
		msg.Key = []byte(uuid.New().String())
	}

	key := string(msg.Key)
	req := msg.Value

	msgState := p.getMessageState(key, req, msg.TopicPartition.Partition)

	// Keep track of which input partition this msgState is mapped to
	// so we know which transactional producer to use.
	if msgState.partition == kafka.PartitionAny {
		msgState.partition = msg.TopicPartition.Partition
	}
}

func (p *Processor) commitTransactionForInputPartition(partition int32) {
	producer, found := p.Producers[partition]
	if !found || producer == nil {
		glog.Fatalf("No producer for input partition %v", partition)
	}

	position := p.getConsumerPosition(partition)
	consumerMetadata, err := p.Consumer.GetConsumerGroupMetadata()
	if err != nil {
		glog.Fatal(fmt.Sprintf("Failed to get consumer group metadata: %v", err))
	}

	err = producer.SendOffsetsToTransaction(nil, position, consumerMetadata)
	if err != nil {
		glog.Errorf(
			"Processor: Failed to send offsets to transaction for input partition %v: %s: aborting transaction",
			partition, err)

		err = producer.AbortTransaction(nil)
		if err != nil {
			glog.Fatal(err)
		}

		// Rewind this input partition to the last committed offset.
		p.rewindConsumerPosition(partition)
	} else {
		err = producer.CommitTransaction(nil)
		if err != nil {
			glog.Errorf(
				"Processor: Failed to commit transaction for input partition %v: %s",
				partition, err)

			err = producer.AbortTransaction(nil)
			if err != nil {
				glog.Fatal(err)
			}

			// Rewind this input partition to the last committed offset.
			p.rewindConsumerPosition(partition)
		}
	}

	// Start a new transaction
	err = producer.BeginTransaction()
	if err != nil {
		glog.Fatal(err)
	}
}

// Run state machine for a single intersection to update light colors.
// Returns true if output messages were produced, else false.
func (p *Processor) intersectionStateMachine(msgState *MessageState) bool {
	changed := false

	// Get the producer for this istate's input partition
	producer := p.Producers[msgState.partition]
	if producer == nil {
		glog.Fatalf("BUG: No producer for key %s partition %v", msgState.key, msgState.partition)
	}

	if msgState.status == ready {
		msgState.stopchan = make(chan struct{})
		msgState.stoppedchan = make(chan struct{})

		msgState.status = delivered
		msgState.lastChange = time.Now()

		go func(msgState *MessageState) { // work in background
			// close the stoppedchan when this func exits
			defer close(msgState.stoppedchan)

			for {
				select {
				default:
					resp, err := p.Handler(msgState.req)
					if err != nil {
						msgState.status = errored
					} else {
						msgState.resp = resp
						msgState.status = processed
						msgState.lastChange = time.Now()
					}
				case <-msgState.stopchan:
					// stop
					return
				}
			}
		}(msgState)
	} else if msgState.status == delivered && time.Since(msgState.lastChange) > 4*time.Second {
		close(msgState.stopchan)
		<-msgState.stoppedchan

		msgState.status = errored
		msgState.lastChange = time.Now()
	} else if msgState.status == processed {
		err := producer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &p.ResponseTopic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msgState.key),
				Value: msgState.resp,
			}, nil)

		if err != nil {
			glog.Fatalf("Failed to produce message: %v", err)
		}

		msgState.status = done
		msgState.lastChange = time.Now()

		changed = true
	} else if msgState.status == errored {
		err := producer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &p.ResponseTopic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msgState.key),
				Value: []byte("handler timed out"),
			}, nil)

		if err != nil {
			glog.Fatalf("Failed to produce message: %v", err)
		}

		msgState.status = done
		msgState.lastChange = time.Now()

		changed = true
	}

	return changed
}

// Startup starts the processor
func (p *Processor) Startup(wg *sync.WaitGroup, termChan chan bool) error {
	defer wg.Done()

	if p.State != online {
		return fmt.Errorf("processor not online")
	}
	glog.Info("Processor starting up...")

	ticker := time.NewTicker(500 * time.Millisecond)

	run := true
	for run {
		select {

		case <-ticker.C:
			// Run intersection state machine(s) periodically
			partitionsToCommit := make(map[int32]bool)

			for _, msgState := range p.MessageStates {
				if p.intersectionStateMachine(msgState) {
					// The handler wants its transaction committed.
					partitionsToCommit[msgState.partition] = true
				}
			}

			// Commit transactions
			for partition := range partitionsToCommit {
				p.commitTransactionForInputPartition(partition)
			}

		case <-termChan:
			run = false

		default:
			// Poll consumer for new messages or rebalance events.
			ev := p.Consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				p.processIngressMessage(e)
			case kafka.Error:
				// Errors are generally just informational.
				glog.Errorf("Consumer error: %s", ev)
			default:
				glog.Infof("Consumer event: %s: ignored", ev)
			}
		}
	}
	return nil
}

// Shutdown safely closes the processor's consumer and producers
func (p *Processor) Shutdown() error {
	if p.State != online {
		return fmt.Errorf("processor already shutdown")
	}

	glog.Info("Processor shutting down...")

	for _, producer := range p.Producers {
		producer.AbortTransaction(nil)
		producer.Close()
	}

	err := p.Consumer.Close()
	if err != nil {
		return err
	}
	glog.Info("Processor successfully shut down")
	return nil
}
