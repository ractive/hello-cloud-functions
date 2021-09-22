// Package helloworld provides a set of Cloud Functions samples.
package eventsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
)

// GOOGLE_CLOUD_PROJECT is a user-set environment variable.
var projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")

// client is a global Pub/Sub client, initialized once per instance.
var client *pubsub.Client

func init() {
	// err is pre-declared to avoid shadowing client.
	var err error

	// client is initialized with context.Background() because it should
	// persist between function invocations.
	client, err = pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
                panic("pubsub.NewClient")
	}
}

type publishEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

type pubSubEvent struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

// PublishMessage publishes a message to Pub/Sub. PublishMessage only works
// with topics that already exist.
func EventSink(w http.ResponseWriter, r *http.Request) {
	// Parse the request body to get the topic name and message.
	p := publishEvent{}

	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		log.Printf("json.NewDecoder: %v", err)
		http.Error(w, "Error parsing request", http.StatusBadRequest)
		return
	}

	if p.Message == "" || p.Event == "" {
		s := "missing 'topic' or 'message' parameter"
		log.Println(s)
		http.Error(w, s, http.StatusBadRequest)
		return
	}

	b := new(bytes.Buffer)
	err := json.NewEncoder(b).Encode(pubSubEvent{
		Event:   p.Event,
		Message: p.Message,
	})
	if err != nil {
		log.Printf("Could not encode json %v", err)
		http.Error(w, "Error encoding json", http.StatusInternalServerError)
		return
	}

	m := &pubsub.Message{
		Data: []byte(b.Bytes()),
	}
	// Publish and Get use r.Context() because they are only needed for this
	// function invocation. If this were a background function, they would use
	// the ctx passed as an argument.
	id, err := client.Topic("events").Publish(r.Context(), m).Get(r.Context())
	if err != nil {
		log.Printf("topic.Publish.Get: %v", err)
		http.Error(w, "Error publishing message", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Message published: %v", id)
}
