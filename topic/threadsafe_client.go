package topic

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type threadsafeClient struct {
	sarama.Client
	*sync.Mutex

	config *sarama.Config
	hosts  []string
}

func newClient(hosts []string, config *sarama.Config) (*threadsafeClient, error) {
	ret := &threadsafeClient{
		Mutex:  new(sync.Mutex),
		config: config,
		hosts:  hosts,
	}

	return ret, ret.ensureConnected()
}

func (t *threadsafeClient) ensureConnected() error {
	t.Lock()
	defer t.Unlock()

	if !t.Closed() {
		return nil
	}

	client, err := sarama.NewClient(t.hosts, t.config)
	if err != nil {
		return errors.Wrap(err, "failed to create Kafka client")
	}

	t.Client = client

	return nil
}
