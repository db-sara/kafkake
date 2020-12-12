package kafkake

// Error is the interface for sending errors through the Kafka client
type Error struct {
	Message error `json:"message"`
}
