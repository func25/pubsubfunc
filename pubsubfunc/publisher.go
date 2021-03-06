package pubsubfunc

import (
	"context"

	"cloud.google.com/go/pubsub"
)

var topics = make(map[string]*pubsub.Topic)

func PullTopic(ctx context.Context, topicId string, numGoroutines int) error {
	if connection.Cmd.IsLocal {
		return nil
	}

	topic := connection.Client.Topic(topicId)
	exist, err := topic.Exists(ctx)
	if err != nil {
		return err
	} else if !exist {
		return ErrTopicNotExist
	} else {
		topics[topicId] = connection.Client.Topic(topicId)
		topics[topicId].PublishSettings.NumGoroutines = numGoroutines
	}

	return nil
}

func PublishMessage(ctx context.Context, topicId string, rawMessage []byte) error {
	if connection.Cmd.IsLocal {
		return nil
	}

	clientTopic, exist := topics[topicId]
	if !exist {
		return ErrPublisherNotExist
	}

	message := pubsub.Message{
		Data: rawMessage,
	}
	result := clientTopic.Publish(ctx, &message)
	_, err := result.Get(ctx)
	return err
}

func CloseTopic(topicId string) error {
	if connection.Cmd.IsLocal {
		return nil
	}

	clientTopic, exist := topics[topicId]
	if !exist {
		return ErrPublisherNotExist
	}
	delete(topics, topicId)
	clientTopic.Stop()

	return nil
}
