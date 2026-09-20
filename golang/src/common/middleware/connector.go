package middleware

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConnector struct {
	connectionSettings ConnSettings
	channel            *amqp.Channel
	connection         *amqp.Connection
}

func NewRabbitConnector(settings ConnSettings) (*RabbitConnector, error) {
	connection, channel, err := openConnection(settings)
	if err != nil {
		return nil, err
	}
	return &RabbitConnector{
		connectionSettings: settings,
		connection:         connection,
		channel:            channel,
	}, nil
}

func openConnection(settings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}
	return conn, ch, nil
}

func (rc *RabbitConnector) declareQueue(queueName string, autoDelete bool, durable bool, exclusive bool, args amqp.Table) (amqp.Queue, error) {
	queue, err := rc.channel.QueueDeclare(
		queueName,  // name
		durable,    // durability
		autoDelete, // delete when unused
		exclusive,  // exclusive
		false,      // no-wait
		args)
	if err != nil {
		return amqp.Queue{}, ErrMessageMiddlewareMessage
	}
	return queue, nil
}

func (rc *RabbitConnector) declareExchange(exchangeName string) error {
	err := rc.channel.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durability
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (rc *RabbitConnector) bindQueue(queue string, key string, exchange string) error {
	err := rc.channel.QueueBind(queue, key, exchange, false, nil)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (rc *RabbitConnector) consumeQueue(
	queue string,
	consumer string,
	callback func(msg Message, ack func(), nack func()),
) error {
	closeChannel := rc.channel.NotifyClose(make(chan *amqp.Error, 1))

	queueChannel, err := rc.channel.Consume(
		queue,    // queue
		consumer, // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	for msg := range queueChannel {
		log.Printf("Received a message: %s", msg.Body)
		message := Message{
			Body: string(msg.Body),
		}
		callback(message, rc.ack(msg), rc.nack(msg))
	}

	select {
	case amqpErr, ok := <-closeChannel:
		if ok && amqpErr != nil {
			return ErrMessageMiddlewareDisconnected
		}
	default:
	}
	return nil
}

func (rc *RabbitConnector) stopConsuming(consumerTag string) error {
	err := rc.channel.Cancel(consumerTag, false)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (rc *RabbitConnector) publish(msg Message, exchange string, routingKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := rc.channel.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (rc *RabbitConnector) closeConnections() error {
	channelErr := rc.channel.Close()
	connectionErr := rc.connection.Close()
	if channelErr != nil || connectionErr != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func (rc *RabbitConnector) nack(msg amqp.Delivery) func() {
	return func() {
		if err := msg.Nack(false, true); err != nil {
			log.Printf("Failed to nack message: %v", err)
		}
	}
}

func (rc *RabbitConnector) ack(msg amqp.Delivery) func() {
	return func() {
		if err := msg.Ack(false); err != nil {
			log.Printf("Failed to ack message: %v", err)
		}
	}
}
