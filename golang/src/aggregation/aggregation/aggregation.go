package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

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
	eofReceived        int
	recordsReceived    int32
	totalRecords       int32
	reportedRecords    int32
	coordinatedRecords int32
}

type Aggregation struct {
	outputQueue          middleware.Middleware
	inputExchange        middleware.Middleware
	coordinationExchange middleware.Middleware
	fruitItemMap         map[int32]map[string]fruititem.FruitItem
	clientStates         map[int32]*clientState
	sumAmount            int
	topSize              int
	mutex                sync.Mutex
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

	coordinationRoutingKey := []string{fmt.Sprintf("%s_count", config.AggregationPrefix)}
	coordinationExchange, err := middleware.CreateExchangeMiddleware(fmt.Sprintf("%s_coordination", config.AggregationPrefix), coordinationRoutingKey, connSettings)
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:          outputQueue,
		inputExchange:        inputExchange,
		coordinationExchange: coordinationExchange,
		fruitItemMap:         map[int32]map[string]fruititem.FruitItem{},
		clientStates:         map[int32]*clientState{},
		sumAmount:            config.SumAmount,
		topSize:              config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	defer aggregation.close()

	coordinationDone := make(chan struct{})
	go func() {
		defer close(coordinationDone)
		err := aggregation.coordinationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleCoordinationMessage(msg, ack, nack)
		})
		if err != nil {
			slog.Error("While consuming coordination exchange", "err", err)
		}
	}()
	go aggregation.handleSignals()

	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming input", "err", err)
	}

	aggregation.coordinationExchange.StopConsuming()
	<-coordinationDone
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.inputExchange.StopConsuming()
	aggregation.coordinationExchange.StopConsuming()
}

func (aggregation *Aggregation) close() {
	aggregation.inputExchange.Close()
	aggregation.coordinationExchange.Close()
	aggregation.outputQueue.Close()
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
	if state.eofReceived < aggregation.sumAmount {
		return nil
	}
	if err := aggregation.reportRecords(clientId); err != nil {
		return err
	}
	// Other shards may have already reported everything, or the client sent no records at all
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
	state := aggregation.getClientState(clientId)
	state.recordsReceived += recordCount
	if state.eofReceived < aggregation.sumAmount {
		return nil
	}
	// Late partial sent by a sum after its flush
	slog.Info("Received records after all EOFs", "clientId", clientId, "records", state.recordsReceived)
	return aggregation.reportRecords(clientId)
}

// reportRecords tells every aggregation how many records of the client this shard received
// since its last report. Shards with nothing new stay silent.
func (aggregation *Aggregation) reportRecords(clientId int32) error {
	state := aggregation.clientStates[clientId]
	newRecords := state.recordsReceived - state.reportedRecords
	if newRecords == 0 {
		return nil
	}
	message, err := inner.SerializeMessage(inner.DataMessage(clientId, []fruititem.FruitItem{}, newRecords))
	if err != nil {
		return err
	}
	if err := aggregation.coordinationExchange.Send(*message); err != nil {
		return err
	}
	state.reportedRecords = state.recordsReceived
	return nil
}

func (aggregation *Aggregation) handleCoordinationMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		return
	}

	aggregation.mutex.Lock()
	defer aggregation.mutex.Unlock()

	state := aggregation.getClientState(message.ClientId)
	state.coordinatedRecords += message.Count
	if err := aggregation.flushIfComplete(message.ClientId); err != nil {
		slog.Error("While flushing client", "clientId", message.ClientId, "err", err)
	}
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
	if state.eofReceived < aggregation.sumAmount || state.coordinatedRecords < state.totalRecords {
		return nil
	}
	slog.Info("Client complete, sending partial top", "clientId", clientId, "records", state.recordsReceived)

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
