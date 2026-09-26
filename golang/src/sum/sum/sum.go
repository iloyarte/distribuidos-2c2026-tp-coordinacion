package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue           middleware.Middleware
	outputExchange       middleware.Middleware
	coordinationExchange middleware.Middleware
	readyQueue           middleware.Middleware
	fruitItemMap         map[int32]map[string]fruititem.FruitItem
	mutex                sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	coordinationRoutingKey := []string{fmt.Sprintf("%s_eof", config.SumPrefix)}
	coordinationExchange, err := middleware.CreateExchangeMiddleware(fmt.Sprintf("%s_coordination", config.SumPrefix), coordinationRoutingKey, connSettings)
	if err != nil {
		outputExchange.Close()
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:           inputQueue,
		outputExchange:       outputExchange,
		coordinationExchange: coordinationExchange,
		fruitItemMap:         map[int32]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		err := sum.coordinationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleCoordinationMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("While consuming coordination exchange", "err", err)
		}
	}()

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if message.IsEOF() {
		if err := sum.handleEndOfRecordMessage(message.ClientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(message); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId int32) error {
	slog.Info("Received End Of Records message", "clientId", clientId)
	message, err := inner.SerializeMessage(inner.EOFMessage(clientId))
	if err != nil {
		return err
	}
	return sum.coordinationExchange.Send(*message)
}

func (sum *Sum) handleCoordinationMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		return
	}

	if err := sum.flushClient(message.ClientId); err != nil {
		slog.Error("While flushing client", "clientId", message.ClientId, "err", err)
	}
}

func (sum *Sum) flushClient(clientId int32) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	slog.Info("Flushing client", "clientId", clientId)
	err := sum.sendFruits(clientId)
	if err != nil {
		return err
	}

	err = sum.sendEOF(clientId)
	if err != nil {
		return err
	}
	delete(sum.fruitItemMap, clientId)
	return nil
}

func (sum *Sum) sendEOF(clientId int32) error {
	message, err := inner.SerializeMessage(inner.EOFMessage(clientId))
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) sendFruits(clientId int32) error {
	for key := range sum.fruitItemMap[clientId] {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[clientId][key]}
		message, err := inner.SerializeMessage(inner.DataMessage(clientId, fruitRecord))
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) handleDataMessage(message inner.FruitMessage) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if _, exists := sum.fruitItemMap[message.ClientId]; !exists {
		sum.fruitItemMap[message.ClientId] = make(map[string]fruititem.FruitItem)
	}
	for _, fruitRecord := range message.Fruits {
		_, ok := sum.fruitItemMap[message.ClientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[message.ClientId][fruitRecord.Fruit] = sum.fruitItemMap[message.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[message.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
