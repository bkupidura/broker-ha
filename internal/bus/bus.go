package bus

import (
	"fmt"
	"log"
	"sync"
)

// Event is used to transport user data over bus.
type Event struct {
	Data        interface{}
	ChannelName string
}

// eventChannel stores subscriptions for channels.
type eventChannel struct {
	subscribers map[string]chan Event
}

// newEventChannel creates new eventChannel.
func newEventChannel() *eventChannel {
	return &eventChannel{
		subscribers: make(map[string]chan Event),
	}
}

// addSubscriber adds new subscriber.
func (ch *eventChannel) addSubscriber(subName string, size int) error {
	if _, ok := ch.subscribers[subName]; ok {
		return fmt.Errorf("subscriber %s already exists", subName)
	}

	ch.subscribers[subName] = make(chan Event, size)

	return nil
}

// delSubscriber removes subscriber.
func (ch *eventChannel) delSubscriber(subName string) {
	delete(ch.subscribers, subName)
}

// Bus should be created by New().
type Bus struct {
	mu       sync.Mutex
	channels map[string]*eventChannel
}

// New creates new Bus instance.
func New() *Bus {
	b := &Bus{
		channels: make(map[string]*eventChannel),
	}
	return b
}

// Subscribe adds new subscriber to channel.
func (b *Bus) Subscribe(channelName, subName string, size int) (chan Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.channels[channelName]; !ok {
		b.channels[channelName] = newEventChannel()
	}

	ch := b.channels[channelName]

	if err := ch.addSubscriber(subName, size); err != nil {
		return nil, err
	}

	return ch.subscribers[subName], nil

}

// Unsubscribe removes subscriber from channel.
// If this is last subscriber channel will be removed.
func (b *Bus) Unsubscribe(channelName, subName string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	channel, ok := b.channels[channelName]
	if !ok {
		return
	}
	channel.delSubscriber(subName)

	if len(channel.subscribers) == 0 {
		delete(b.channels, channelName)
	}
}

// Publish data to channel.
func (b *Bus) Publish(channelName string, data interface{}) error {
	channel, ok := b.channels[channelName]
	if !ok {
		return fmt.Errorf("channel %s dont exists", channelName)
	}
	e := Event{
		ChannelName: channelName,
		Data:        data,
	}

	for subName, subscriber := range channel.subscribers {
		if cap(subscriber) > 0 && len(subscriber) >= cap(subscriber) {
			log.Printf("channel %s for subscriber %s is full, not publishing new messages", channelName, subName)
			continue
		}
		subscriber <- e
	}
	return nil
}
