package pubsubfunc

import "errors"

var ErrSubscriberNotExist = errors.New("[pubsub] - subscriber not exist")
var ErrPublisherNotExist = errors.New("[pubsub] - publisher not exist")
var ErrTopicNotExist = errors.New("[pubsub] - topic not exist")
var ErrSubscriptionNotExist = errors.New("[pubsub] - subscription not exist")
