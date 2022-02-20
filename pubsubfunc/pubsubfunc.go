package pubsubfunc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type HandleMsg func([]byte) interface{}

var subscribers = make(map[string]*Subscriber)
var connection PubsubConnection

// Connect to google pubsub of projectID
func Connect(ctx context.Context, cmd ConnectCmd) (*pubsub.Client, error) {
	var err error
	if cmd.Opts == nil {
		cmd.Opts = []option.ClientOption{}
	}
	if connection.Client == nil && !cmd.IsLocal {
		connection.Client, err = pubsub.NewClient(ctx, cmd.ProjectID, cmd.Opts...)
		if err != nil {
			return nil, err
		}
	}

	return connection.Client, nil
}

// Validate, create topic and subscription if it's exist
func Validate(ctx context.Context, subId string, topicId string) error {
	if connection.Cmd.IsLocal {
		return nil
	}

	topic := connection.Client.Topic(topicId)
	if exist, err := topic.Exists(ctx); err != nil {
		return err
	} else if !exist {
		_, err := connection.Client.CreateTopic(ctx, topicId)
		if err != nil {
			return err
		}
	}

	sub := connection.Client.Subscription(subId)
	if exist, err := sub.Exists(ctx); err != nil {
		return err
	} else if !exist {
		if _, err := connection.Client.CreateSubscription(ctx, subId, pubsub.SubscriptionConfig{
			Topic: topic,
		}); err != nil {
			return err
		}
	}

	return nil
}
