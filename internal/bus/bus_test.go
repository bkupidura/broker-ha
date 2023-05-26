package bus

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewEventChannel(t *testing.T) {
	ch := newEventChannel()
	require.Equal(t, 0, len(ch.subscribers))
}

func TestAddSubscriber(t *testing.T) {
	tests := []struct {
		inputEventChannel        *eventChannel
		inputSubscriptionName    string
		inputSubscriptionSize    int
		expectedError            error
		expectedSubscribers      int
		expectedSubscriptionSize int
	}{
		{
			inputEventChannel: &eventChannel{
				subscribers: make(map[string]chan Event),
			},
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedSubscribers:      1,
			expectedSubscriptionSize: 1024,
		},
		{
			inputEventChannel: &eventChannel{
				subscribers: map[string]chan Event{
					"existing": make(chan Event),
				},
			},
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedSubscribers:      2,
			expectedSubscriptionSize: 1024,
		},
		{
			inputEventChannel: &eventChannel{
				subscribers: map[string]chan Event{
					"existing": make(chan Event),
				},
			},
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    0,
			expectedSubscribers:      2,
			expectedSubscriptionSize: 0,
		},
		{
			inputEventChannel: &eventChannel{
				subscribers: map[string]chan Event{
					"test": make(chan Event),
				},
			},
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedSubscribers:      1,
			expectedSubscriptionSize: 0,
			expectedError:            errors.New("subscriber test already exists"),
		},
	}

	for _, test := range tests {
		err := test.inputEventChannel.addSubscriber(test.inputSubscriptionName, test.inputSubscriptionSize)
		require.Equal(t, test.expectedError, err)
		require.Equal(t, test.expectedSubscribers, len(test.inputEventChannel.subscribers))
		require.Equal(t, test.expectedSubscriptionSize, cap(test.inputEventChannel.subscribers[test.inputSubscriptionName]))
	}
}

func TestDelSubscriber(t *testing.T) {
	tests := []struct {
		inputEventChannel        *eventChannel
		inputSubscriptionName    string
		expectedSubscribers      int
		expectedSubscriptionSize int
	}{
		{
			inputEventChannel: &eventChannel{
				subscribers: make(map[string]chan Event),
			},
			inputSubscriptionName: "test",
			expectedSubscribers:   0,
		},
		{
			inputEventChannel: &eventChannel{
				subscribers: map[string]chan Event{
					"existing": make(chan Event),
				},
			},
			inputSubscriptionName: "test",
			expectedSubscribers:   1,
		},
		{
			inputEventChannel: &eventChannel{
				subscribers: map[string]chan Event{
					"test": make(chan Event),
				},
			},
			inputSubscriptionName: "test",
			expectedSubscribers:   0,
		},
	}
	for _, test := range tests {
		test.inputEventChannel.delSubscriber(test.inputSubscriptionName)
		require.Equal(t, test.expectedSubscribers, len(test.inputEventChannel.subscribers))
	}

}

func TestNew(t *testing.T) {
	b := New()
	require.Equal(t, 0, len(b.channels))
}

func TestSubscribe(t *testing.T) {
	tests := []struct {
		inputBus                 *Bus
		inputChannelName         string
		inputSubscriptionName    string
		inputSubscriptionSize    int
		expectedError            error
		expectedChannels         int
		expectedSubscribers      int
		expectedSubscriptionSize int
	}{
		{
			inputBus:                 New(),
			inputChannelName:         "test",
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedChannels:         1,
			expectedSubscribers:      1,
			expectedSubscriptionSize: 1024,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"existing": newEventChannel(),
				},
			},
			inputChannelName:         "test",
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedChannels:         2,
			expectedSubscribers:      1,
			expectedSubscriptionSize: 1024,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"test": {
						subscribers: map[string]chan Event{
							"existing": make(chan Event),
						},
					},
				},
			},
			inputChannelName:         "test",
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedChannels:         1,
			expectedSubscribers:      2,
			expectedSubscriptionSize: 1024,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"test": {
						subscribers: map[string]chan Event{
							"test": make(chan Event),
						},
					},
				},
			},
			inputChannelName:         "test",
			inputSubscriptionName:    "test",
			inputSubscriptionSize:    1024,
			expectedChannels:         1,
			expectedSubscribers:      1,
			expectedSubscriptionSize: 0,
			expectedError:            errors.New("subscriber test already exists"),
		},
	}
	for _, test := range tests {
		ch, err := test.inputBus.Subscribe(test.inputChannelName, test.inputSubscriptionName, test.inputSubscriptionSize)
		require.Equal(t, test.expectedError, err)
		require.Equal(t, test.expectedChannels, len(test.inputBus.channels))
		require.Equal(t, test.expectedSubscribers, len(test.inputBus.channels[test.inputChannelName].subscribers))
		require.Equal(t, test.expectedSubscriptionSize, cap(ch))
	}
}

func TestUnsubscribe(t *testing.T) {
	tests := []struct {
		inputBus              *Bus
		inputChannelName      string
		inputSubscriptionName string
		expectedChannels      int
		expectedSubscribers   int
	}{
		{
			inputBus:              New(),
			inputChannelName:      "test",
			inputSubscriptionName: "test",
			expectedChannels:      0,
			expectedSubscribers:   0,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"existing": newEventChannel(),
				},
			},
			inputChannelName:      "test",
			inputSubscriptionName: "test",
			expectedChannels:      1,
			expectedSubscribers:   0,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"test": {
						subscribers: map[string]chan Event{
							"existing": make(chan Event),
						},
					},
				},
			},
			inputChannelName:      "test",
			inputSubscriptionName: "test",
			expectedChannels:      1,
			expectedSubscribers:   1,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"test": {
						subscribers: map[string]chan Event{
							"test": make(chan Event),
						},
					},
				},
			},
			inputChannelName:      "test",
			inputSubscriptionName: "test",
			expectedChannels:      0,
			expectedSubscribers:   0,
		},
		{
			inputBus: &Bus{
				channels: map[string]*eventChannel{
					"test": {
						subscribers: map[string]chan Event{
							"existing": make(chan Event),
							"test":     make(chan Event),
						},
					},
				},
			},
			inputChannelName:      "test",
			inputSubscriptionName: "test",
			expectedChannels:      1,
			expectedSubscribers:   1,
		},
	}
	for _, test := range tests {
		test.inputBus.Unsubscribe(test.inputChannelName, test.inputSubscriptionName)
		require.Equal(t, test.expectedChannels, len(test.inputBus.channels))
		if test.expectedSubscribers > 0 {
			require.Equal(t, test.expectedSubscribers, len(test.inputBus.channels[test.inputChannelName].subscribers))
		}
	}
}

func TestPublish(t *testing.T) {
	tests := []struct {
		inputSubscribers func(*Bus, chan map[string]int)
		inputPublish     func(*Bus)
		expectedOutput   map[string]int
	}{
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				s1, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				go func() {
					r := make(map[string]int)
					for {
						select {
						case e := <-s1:
							require.Equal(t, "test", e.ChannelName)
							r["s1"]++
						case <-time.After(5 * time.Millisecond):
							output <- r
							return
						}
					}
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Nil(t, err)
			},
			expectedOutput: map[string]int{"s1": 1},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				s1, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				go func() {
					r := make(map[string]int)
					for {
						select {
						case e := <-s1:
							require.Equal(t, "test", e.ChannelName)
							r["s1"]++
						case <-time.After(5 * time.Millisecond):
							output <- r
							return
						}
					}
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Nil(t, err)
				err = b.Publish("test", "test")
				require.Nil(t, err)
			},
			expectedOutput: map[string]int{"s1": 2},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				s1, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				s2, err := b.Subscribe("test", "s2", 1024)
				require.Nil(t, err)
				go func() {
					r := make(map[string]int)
					for {
						select {
						case e := <-s1:
							require.Equal(t, "test", e.ChannelName)
							r["s1"]++
						case e := <-s2:
							require.Equal(t, "test", e.ChannelName)
							r["s2"]++
						case <-time.After(5 * time.Millisecond):
							output <- r
							return
						}
					}
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Nil(t, err)
				err = b.Publish("test", "test")
				require.Nil(t, err)
			},
			expectedOutput: map[string]int{"s1": 2, "s2": 2},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				s1, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				s2, err := b.Subscribe("test2", "s2", 1024)
				require.Nil(t, err)
				go func() {
					r := make(map[string]int)
					for {
						select {
						case e := <-s1:
							require.Equal(t, "test", e.ChannelName)
							r["s1"]++
						case e := <-s2:
							require.Equal(t, "test2", e.ChannelName)
							r["s2"]++
						case <-time.After(5 * time.Millisecond):
							output <- r
							return
						}
					}
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Nil(t, err)
				err = b.Publish("test", "test")
				require.Nil(t, err)
			},
			expectedOutput: map[string]int{"s1": 2},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				s1, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				s2, err := b.Subscribe("test", "s2", 1)
				require.Nil(t, err)
				go func() {
					r := make(map[string]int)
					e := <-s2
					require.Equal(t, "test", e.ChannelName)
					r["s2"]++
					for {
						select {
						case e := <-s1:
							require.Equal(t, "test", e.ChannelName)
							r["s1"]++
						case <-time.After(5 * time.Millisecond):
							output <- r
							return
						}
					}
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Nil(t, err)
				err = b.Publish("test", "test")
				require.Nil(t, err)
			},
			expectedOutput: map[string]int{"s1": 2, "s2": 1},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				ch, err := b.Subscribe("test", "s1", 1024)
				require.Nil(t, err)
				go func() {
					var mu sync.Mutex
					r := make(map[string]int)
					var wg sync.WaitGroup
					wg.Add(3)
					w := func(r map[string]int, n string) {
						e := <-ch
						require.Equal(t, "test", e.ChannelName)
						mu.Lock()
						defer mu.Unlock()
						r[n]++
						wg.Done()
					}
					go w(r, "s1")
					go w(r, "s2")
					go w(r, "s3")
					wg.Wait()
					output <- r
				}()
			},
			inputPublish: func(b *Bus) {
				go func(b *Bus) {
					err := b.Publish("test", "test")
					require.Nil(t, err)
					err = b.Publish("test", "test")
					require.Nil(t, err)
					err = b.Publish("test", "test")
					require.Nil(t, err)
				}(b)
			},
			expectedOutput: map[string]int{"s1": 1, "s2": 1, "s3": 1},
		},
		{
			inputSubscribers: func(b *Bus, output chan map[string]int) {
				go func() {
					r := make(map[string]int)
					output <- r
				}()
			},
			inputPublish: func(b *Bus) {
				err := b.Publish("test", "test")
				require.Equal(t, errors.New("channel test dont exists"), err)
			},
			expectedOutput: map[string]int{},
		},
	}

	for _, test := range tests {
		b := New()
		ch := make(chan map[string]int)
		test.inputSubscribers(b, ch)
		test.inputPublish(b)

		received := <-ch

		require.Equal(t, test.expectedOutput, received)

	}
}
