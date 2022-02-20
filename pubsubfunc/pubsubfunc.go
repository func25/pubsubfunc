package pubsubfunc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type HandleMsg func([]byte) interface{}

var subscribers = make(map[string]*Subscriber)
var client *pubsub.Client

// Connect to google pubsub of projectID
func Connect(ctx context.Context, projectID string, opts ...option.ClientOption) (*pubsub.Client, error) {
	var err error
	if client == nil {
		client, err = pubsub.NewClient(ctx, projectID, opts...)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
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
