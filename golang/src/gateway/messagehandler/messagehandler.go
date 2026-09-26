package messagehandler

import (
	"log/slog"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var clientIdCounter atomic.Int32

type MessageHandler struct {
	clientId int32
}

func NewMessageHandler() MessageHandler {
	clientId := clientIdCounter.Add(1)
	slog.Debug("Created new message handler", "clientId", clientId)
	return MessageHandler{clientId: clientId}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	message := inner.DataMessage(messageHandler.clientId, []fruititem.FruitItem{fruitRecord})
	return inner.SerializeMessage(message)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	message := inner.EOFMessage(messageHandler.clientId)
	return inner.SerializeMessage(message)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if fruitMessage.ClientId != messageHandler.clientId || fruitMessage.IsEOF() {
		return nil, nil
	}

	return fruitMessage.Fruits, nil
}
