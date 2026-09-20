package middleware

type ExchangeMiddleware struct {
	exchangeName  string
	connector     *RabbitConnector
	exchangeQueue string
	routingKeys   []string
}

func NewExchangeMiddleware(connectionSettings ConnSettings, exchangeName string, keys []string) (Middleware, error) {
	connector, err := NewRabbitConnector(connectionSettings)
	if err != nil {
		return nil, err
	}
	err = connector.declareExchange(exchangeName)
	if err != nil {
		return nil, err
	}

	return &ExchangeMiddleware{
		exchangeName: exchangeName,
		connector:    connector,
		routingKeys:  keys,
	}, nil
}

func (eMiddleware *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queue, err := eMiddleware.connector.declareQueue("", true, false, true, nil)
	if err != nil {
		return err
	}
	eMiddleware.exchangeQueue = queue.Name
	err = eMiddleware.bindQueues()
	if err != nil {
		return err
	}
	return eMiddleware.connector.consumeQueue(eMiddleware.exchangeQueue, eMiddleware.exchangeName, callbackFunc)
}

func (eMiddleware *ExchangeMiddleware) bindQueues() error {
	for _, key := range eMiddleware.routingKeys {
		err := eMiddleware.connector.bindQueue(eMiddleware.exchangeQueue, key, eMiddleware.exchangeName)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (eMiddleware *ExchangeMiddleware) StopConsuming() error {
	return eMiddleware.connector.stopConsuming(eMiddleware.exchangeName)
}

func (eMiddleware *ExchangeMiddleware) Send(msg Message) error {
	for _, key := range eMiddleware.routingKeys {
		err := eMiddleware.connector.publish(msg, eMiddleware.exchangeName, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (eMiddleware *ExchangeMiddleware) Close() error {
	err := eMiddleware.connector.closeConnections()
	if err != nil {
		return err
	}
	return nil
}
