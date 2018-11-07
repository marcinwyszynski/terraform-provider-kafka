package topic

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type clientFactory struct {
	config *sarama.Config
	hosts  []string
}

func (f *clientFactory) build() (sarama.Client, *sarama.Broker, error) {
	client, err := sarama.NewClient(f.hosts, f.config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create Kafka client")
	}

	controller, err := client.Controller()
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get controller")
	}

	if err := controller.Open(client.Config()); err != nil {
		return nil, nil, errors.Wrap(err, "could not open controller connection")
	}

	return client, controller, nil
}
