package pubsubfunc

import (
	"context"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type PubsubConnection struct {
	Client *pubsub.Client
	Cmd    ConnectCmd
}

type ConnectCmd struct {
	ProjectID string
	IsLocal   bool
	Opts      []option.ClientOption
}

type Subscriber struct {
	Subscription *pubsub.Subscription
	Close        context.CancelFunc
}

func (s Subscriber) Subscribe(ctx context.Context, cmd SubscribeCmd) error {
	if connection.Cmd.IsLocal {
		return nil
	}

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

// Subscribe creates a connection and handler to handle message that received on subscription
func Subscribe(ctx context.Context, cmd SubscribeCmd, isBlockThread bool) error {
	if connection.Cmd.IsLocal {
		return nil
	}

	if v, exist := subscribers[cmd.SubId]; exist && v != nil {
		log.Printf("[warning][pubsub] %v have been existed in keys map\n", cmd.SubId)
		return nil
	}

	subscription := connection.Client.Subscription(cmd.SubId)
	if ok, err := subscription.Exists(ctx); !ok || err != nil {
		if err != nil {
			return err
		} else {
			return ErrSubscriptionNotExist
		}
	}

	cancelCtx, cancelFunc := context.WithCancel(ctx)

	var subscriber = &Subscriber{
		Subscription: subscription,
		Close:        cancelFunc,
	}

	subscribers[cmd.SubId] = subscriber

	if isBlockThread {
		if err := subscriber.Subscribe(cancelCtx, cmd); err != nil {
			return err
		}
	} else {
		go subscriber.Subscribe(cancelCtx, cmd)
	}

	return nil
}

// CloseSubscriber, close subscriber's connection
func CloseSubscriber(subID string) {
	if connection.Cmd.IsLocal {
		return
	}

	subscriber, exist := subscribers[subID]
	if exist {
		subscriber.Close()
		delete(subscribers, subID)
	} else {
		log.Printf("[warning][pubsub][close-connection] cannot find subscription %v to close\n", subID)
	}
}
