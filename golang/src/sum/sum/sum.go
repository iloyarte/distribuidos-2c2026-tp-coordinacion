package sum

import (
	"fmt"
	"hash/fnv"
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
	outputExchanges      []middleware.Middleware
	coordinationExchange middleware.Middleware
	fruitItemMap         map[int32]map[string]fruititem.FruitItem
	processedRecords     map[int32][]int32
	sentRecords          map[int32]int32
	aggregationAmount    int
	mutex                sync.Mutex
}

func closeAll(middlewares []middleware.Middleware) {
	for _, m := range middlewares {
		m.Close()
	}
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, 0, config.AggregationAmount)
	for i := range config.AggregationAmount {
		routingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, routingKey, connSettings)
		if err != nil {
			closeAll(outputExchanges)
			inputQueue.Close()
			return nil, err
		}
		outputExchanges = append(outputExchanges, outputExchange)
	}

	coordinationRoutingKey := []string{fmt.Sprintf("%s_eof", config.SumPrefix)}
	coordinationExchange, err := middleware.CreateExchangeMiddleware(fmt.Sprintf("%s_coordination", config.SumPrefix), coordinationRoutingKey, connSettings)
	if err != nil {
		closeAll(outputExchanges)
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:           inputQueue,
		outputExchanges:      outputExchanges,
		coordinationExchange: coordinationExchange,
		fruitItemMap:         map[int32]map[string]fruititem.FruitItem{},
		processedRecords:     map[int32][]int32{},
		sentRecords:          map[int32]int32{},
		aggregationAmount:    config.AggregationAmount,
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
		if err := sum.handleEndOfRecordMessage(message); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(message); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(eofMessage inner.FruitMessage) error {
	slog.Info("Received End Of Records message", "clientId", eofMessage.ClientId, "total", eofMessage.Total)
	message, err := inner.SerializeMessage(inner.EOFMessage(eofMessage.ClientId, eofMessage.Total))
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

	if err := sum.flushClient(message.ClientId, message.Total); err != nil {
		slog.Error("While flushing client", "clientId", message.ClientId, "err", err)
	}
}

func (sum *Sum) flushClient(clientId int32, total int32) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	sum.sentRecords[clientId] = total
	slog.Info("Flushing client", "clientId", clientId, "records", sum.processedRecords[clientId])
	if err := sum.sendPartialResults(clientId); err != nil {
		return err
	}
	return sum.sendEOF(clientId, total)
}

func (sum *Sum) sendPartialResults(clientId int32) error {
	processed, exists := sum.processedRecords[clientId]
	if !exists {
		return nil
	}

	buckets := make([][]fruititem.FruitItem, sum.aggregationAmount)
	for _, fruitRecord := range sum.fruitItemMap[clientId] {
		i := hashFruit(fruitRecord.Fruit, sum.aggregationAmount)
		buckets[i] = append(buckets[i], fruitRecord)
	}
	for i, outputExchange := range sum.outputExchanges {
		if processed[i] == 0 {
			continue
		}
		message, err := inner.SerializeMessage(inner.DataMessage(clientId, buckets[i], processed[i]))
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	delete(sum.fruitItemMap, clientId)
	delete(sum.processedRecords, clientId)
	return nil
}

func (sum *Sum) sendEOF(clientId int32, total int32) error {
	message, err := inner.SerializeMessage(inner.EOFMessage(clientId, total))
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for _, outputExchange := range sum.outputExchanges {
		if err := outputExchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
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
	if _, exists := sum.processedRecords[message.ClientId]; !exists {
		sum.processedRecords[message.ClientId] = make([]int32, sum.aggregationAmount)
	}
	for _, fruitRecord := range message.Fruits {
		_, ok := sum.fruitItemMap[message.ClientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[message.ClientId][fruitRecord.Fruit] = sum.fruitItemMap[message.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[message.ClientId][fruitRecord.Fruit] = fruitRecord
		}
		fruitBucket := hashFruit(fruitRecord.Fruit, sum.aggregationAmount)
		sum.processedRecords[message.ClientId][fruitBucket]++
	}

	if _, flushed := sum.sentRecords[message.ClientId]; flushed {
		slog.Info("Forwarding record received after flush", "clientId", message.ClientId)
		return sum.sendPartialResults(message.ClientId)
	}
	return nil
}

func hashFruit(fruit string, aggregationAmount int) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(fruit))
	return int(hasher.Sum32() % uint32(aggregationAmount))
}
