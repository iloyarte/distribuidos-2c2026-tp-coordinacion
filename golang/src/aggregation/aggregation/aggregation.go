package aggregation

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientState struct {
	eofReceived     int
	recordsReceived int32
	totalRecords    int32
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[int32]map[string]fruititem.FruitItem
	clientStates  map[int32]*clientState
	sumAmount     int
	topSize       int
	mutex         sync.Mutex
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[int32]map[string]fruititem.FruitItem{},
		clientStates:  map[int32]*clientState{},
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if message.IsEOF() {
		if err := aggregation.handleEndOfRecordsMessage(message.ClientId, message.Total); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := aggregation.handleDataMessage(message.ClientId, message.Fruits, message.Count); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId int32, totalRecords int32) error {
	aggregation.mutex.Lock()
	defer aggregation.mutex.Unlock()

	state := aggregation.getClientState(clientId)
	state.eofReceived++
	state.totalRecords = totalRecords
	slog.Info("Received End Of Records message", "clientId", clientId, "eofReceived", state.eofReceived, "expected", aggregation.sumAmount)
	return aggregation.flushIfComplete(clientId)
}

func (aggregation *Aggregation) handleDataMessage(clientId int32, fruitRecords []fruititem.FruitItem, recordCount int32) error {
	aggregation.mutex.Lock()
	defer aggregation.mutex.Unlock()

	if _, exists := aggregation.fruitItemMap[clientId]; !exists {
		aggregation.fruitItemMap[clientId] = make(map[string]fruititem.FruitItem)
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[clientId][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = aggregation.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	aggregation.getClientState(clientId).recordsReceived += recordCount
	return aggregation.flushIfComplete(clientId)
}

func (aggregation *Aggregation) getClientState(clientId int32) *clientState {
	state, exists := aggregation.clientStates[clientId]
	if !exists {
		state = &clientState{}
		aggregation.clientStates[clientId] = state
	}
	return state
}

func (aggregation *Aggregation) flushIfComplete(clientId int32) error {
	state := aggregation.clientStates[clientId]
	if state.eofReceived < aggregation.sumAmount || state.recordsReceived < state.totalRecords {
		return nil
	}
	slog.Info("Client complete, sending top", "clientId", clientId, "records", state.recordsReceived)

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	message, err := inner.SerializeMessage(inner.DataMessage(clientId, fruitTopRecords, state.recordsReceived))
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeMessage(inner.EOFMessage(clientId, state.totalRecords))
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.fruitItemMap, clientId)
	delete(aggregation.clientStates, clientId)
	return nil
}

func (aggregation *Aggregation) buildFruitTop(clientId int32) []fruititem.FruitItem {

	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap[clientId]))
	for _, item := range aggregation.fruitItemMap[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
