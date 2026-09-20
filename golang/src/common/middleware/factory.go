package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	return NewQueueMiddleware(connectionSettings, queueName)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	return NewExchangeMiddleware(connectionSettings, exchange, keys)
}
