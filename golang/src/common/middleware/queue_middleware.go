package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connector *RabbitConnector
	queueName string
}

func NewQueueMiddleware(connectionSettings ConnSettings, queueName string) (Middleware, error) {
	connector, err := NewRabbitConnector(connectionSettings)
	if err != nil {
		return nil, err
	}
	err = declareQueue(connector, queueName)
	if err != nil {
		return nil, err
	}
	return &QueueMiddleware{
		connector: connector,
		queueName: queueName,
	}, nil
}

func declareQueue(connector *RabbitConnector, queueName string) error {
	_, err := connector.declareQueue(
		queueName,
		false,
		true,
		false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	return err
}

func (qMiddleware *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return qMiddleware.connector.consumeQueue(qMiddleware.queueName, qMiddleware.queueName, callbackFunc)
}

func (qMiddleware *QueueMiddleware) StopConsuming() error {
	return qMiddleware.connector.stopConsuming(qMiddleware.queueName)
}

func (qMiddleware *QueueMiddleware) Send(msg Message) error {
	return qMiddleware.connector.publish(msg, "", qMiddleware.queueName)
}

func (qMiddleware *QueueMiddleware) Close() error {
	return qMiddleware.connector.closeConnections()
}
