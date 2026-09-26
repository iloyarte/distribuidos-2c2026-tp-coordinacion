package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type FruitMessage struct {
	MessageType MessageType           `json:"messageType"`
	ClientId    int32                 `json:"clientId"`
	Fruits      []fruititem.FruitItem `json:"fruits"`
}
type MessageType uint8

const (
	MessageTypeData MessageType = iota + 1
	MessageTypeEOF
)

func DataMessage(clientId int32, fruits []fruititem.FruitItem) FruitMessage {
	return FruitMessage{
		MessageType: MessageTypeData,
		ClientId:    clientId,
		Fruits:      fruits,
	}
}

func EOFMessage(clientId int32) FruitMessage {
	return FruitMessage{
		MessageType: MessageTypeEOF,
		ClientId:    clientId,
		Fruits:      []fruititem.FruitItem{},
	}
}

func deserializeJson(message []byte) (FruitMessage, error) {
	var data FruitMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return FruitMessage{}, err
	}
	return data, nil
}

func SerializeMessage(fruitMessage FruitMessage) (*middleware.Message, error) {
	body, err := json.Marshal(fruitMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (FruitMessage, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return FruitMessage{}, err
	}

	if data.Fruits == nil {
		data.Fruits = []fruititem.FruitItem{}
	}

	return data, nil
}

func (fruitMessage FruitMessage) IsEOF() bool {
	return fruitMessage.MessageType == MessageTypeEOF
}
