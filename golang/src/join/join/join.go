package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientState struct {
	partialTops  []fruititem.FruitItem
	eofsReceived int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	clientStates      map[int32]*clientState
	aggregationAmount int
	topSize           int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		clientStates:      map[int32]*clientState{},
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) Run() {
	defer join.close()

	go join.handleSignals()

	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming input", "err", err)
	}
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.StopConsuming()
}

func (join *Join) close() {
	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	message, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	state, exists := join.clientStates[message.ClientId]
	if !exists {
		state = &clientState{}
		join.clientStates[message.ClientId] = state
	}

	if !message.IsEOF() {
		state.partialTops = append(state.partialTops, message.Fruits...)
		return
	}

	state.eofsReceived++
	slog.Info("Received partial top", "clientId", message.ClientId, "eofsReceived", state.eofsReceived, "expected", join.aggregationAmount)
	if state.eofsReceived < join.aggregationAmount {
		return
	}
	if err := join.sendTop(message.ClientId, state.partialTops); err != nil {
		slog.Error("While sending top", "clientId", message.ClientId, "err", err)
	}
	delete(join.clientStates, message.ClientId)
}

func (join *Join) sendTop(clientId int32, fruitItems []fruititem.FruitItem) error {
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	message, err := inner.SerializeMessage(inner.DataMessage(clientId, fruitItems[:finalTopSize], 0))
	if err != nil {
		return err
	}
	return join.outputQueue.Send(*message)
}
