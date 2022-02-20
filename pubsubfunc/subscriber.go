package pubsubfunc

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type ConnectionConfig struct {
	ProjectID string
	TopicID   string
	SubID     string
}

type Subscriber struct {
	Subscription *pubsub.Subscription
	Close        context.CancelFunc
}

func (s Subscriber) Subscribe(ctx context.Context, cmd SubscribeCmd) error {
	return s.Subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		result := cmd.MsgHandler(msg.Data)
		if cmd.Callback != nil {
			cmd.Callback(result)
		}
		msg.Ack()
	})
}

// SubscribeCmd use for create subscriber
type SubscribeCmd struct {
	SubId      string
	MsgHandler func([]byte) interface{}
	Callback   func(interface{})
}
