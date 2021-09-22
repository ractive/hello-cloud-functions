// Package helloworld provides a set of Cloud Functions samples.
package eventconsumer

import (
	"context"
	"encoding/json"
	"log"
	"strings"
)

type pubSubEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

func EventConsumer(ctx context.Context, m PubSubMessage) error {
	p := pubSubEvent {}
	if err := json.NewDecoder(strings.NewReader(string(m.Data))).Decode(&p); err != nil {
		log.Printf("json.NewDecoder: %v", err)
		panic("Could not decode messsage")
	}
	log.Printf("Event: type = %s, message = %s", p.Event, p.Message)
	return nil
}
