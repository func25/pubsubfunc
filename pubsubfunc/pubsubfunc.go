package pubsubfunc

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type HandleMsg func([]byte) interface{}

var subscriberMap = make(map[string]*Subscriber)

var client *pubsub.Client

// Connect to google pubsub of projectID
func Connect(ctx context.Context, projectID string, opts ...option.ClientOption) (*pubsub.Client, error) {
	var err error
	if client == nil {
		client, err = pubsub.NewClient(ctx, projectID, opts...)
		if err != nil {
			return nil, fmt.Errorf("pubsub.NewClient %v", err)
		}
	}
	return client, nil
}

// Subscribe creates a connection and handler to handle message that received on subscription
func Subscribe(ctx context.Context, cmd SubscribeCmd, isBlockThread bool) error {
	if v, exist := subscriberMap[cmd.SubId]; exist && v != nil {
		log.Printf("[pubsub] %v have been existed in keys map\n", cmd.SubId)
		return nil
	}

	subscription := client.Subscription(cmd.SubId)
	if ok, err := subscription.Exists(ctx); !ok || err != nil {
		if err != nil {
			return err
		} else {
			return ErrNotExists
		}
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)

	var subscriber = &Subscriber{
		Subscription: subscription,
		Close:        cancelFunc,
	}

	subscriberMap[cmd.SubId] = subscriber

	if isBlockThread {
		if err := subscriber.Subscribe(cancelCtx, cmd); err != nil {
			return err
		}
	} else {
		go subscriber.Subscribe(cancelCtx, cmd)
	}

	return nil
}

// Validate, create topic and subscription if it's exist
func Validate(ctx context.Context, subId string, topicId string) error {
	topic := client.Topic(topicId)
	if exist, err := topic.Exists(ctx); err != nil {
		return err
	} else if !exist {
		_, err := client.CreateTopic(ctx, topicId)
		if err != nil {
			return err
		}
	}

	sub := client.Subscription(subId)
	if exist, err := sub.Exists(ctx); err != nil {
		return err
	} else if !exist {
		if _, err := client.CreateSubscription(ctx, subId, pubsub.SubscriptionConfig{
			Topic: topic,
		}); err != nil {
			return err
		}
	}

	return nil
}

// CloseConnection, close subscriber's connection
func CloseConnection(subID string) {
	subscriber, exist := subscriberMap[subID]
	if exist {
		subscriber.Close()
		delete(subscriberMap, subID)
	} else {
		log.Printf("[pubsub][close-connection] cannot find subscription %v to close\n", subID)
	}
}
