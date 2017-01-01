// Package tracker provides a service to collect and provision metrics about
// executed CLG trees.
package tracker

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/the-anna-project/connection"
	"github.com/the-anna-project/context"
	clgtreeid "github.com/the-anna-project/context/clg/tree/id"
	destinationid "github.com/the-anna-project/context/destination/id"
	destinationname "github.com/the-anna-project/context/destination/name"
	sourceids "github.com/the-anna-project/context/source/ids"
	sourcenames "github.com/the-anna-project/context/source/names"
	"github.com/the-anna-project/event"
	"github.com/the-anna-project/instrumentor"
	"github.com/the-anna-project/peer"
	"github.com/the-anna-project/storage"
	"github.com/the-anna-project/worker"
)

// ServiceConfig represents the configuration used to create a new CLG service.
type ServiceConfig struct {
	// Dependencies.
	ConnectionService      connection.Service
	EventCollection        *event.Collection
	InstrumentorCollection *instrumentor.Collection
	PeerCollection         *peer.Collection
	StorageCollection      *storage.Collection
	WorkerService          worker.Service
}

// DefaultServiceConfig provides a default configuration to create a new CLG
// service by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var connectionService connection.Service
	{
		connectionConfig := connection.DefaultServiceConfig()
		connectionService, err = connection.NewService(connectionConfig)
		if err != nil {
			panic(err)
		}
	}

	var eventCollection *event.Collection
	{
		eventConfig := event.DefaultCollectionConfig()
		eventCollection, err = event.NewCollection(eventConfig)
		if err != nil {
			panic(err)
		}
	}

	var instrumentorCollection *instrumentor.Collection
	{
		instrumentorConfig := instrumentor.DefaultCollectionConfig()
		instrumentorCollection, err = instrumentor.NewCollection(instrumentorConfig)
		if err != nil {
			panic(err)
		}
	}

	var peerCollection *peer.Collection
	{
		peerConfig := peer.DefaultCollectionConfig()
		peerCollection, err = peer.NewCollection(peerConfig)
		if err != nil {
			panic(err)
		}
	}

	var storageCollection *storage.Collection
	{
		storageConfig := storage.DefaultCollectionConfig()
		storageCollection, err = storage.NewCollection(storageConfig)
		if err != nil {
			panic(err)
		}
	}

	var workerService worker.Service
	{
		workerConfig := worker.DefaultServiceConfig()
		workerService, err = worker.NewService(workerConfig)
		if err != nil {
			panic(err)
		}
	}

	config := ServiceConfig{
		// Dependencies.
		ConnectionService:      connectionService,
		EventCollection:        eventCollection,
		InstrumentorCollection: instrumentorCollection,
		PeerCollection:         peerCollection,
		StorageCollection:      storageCollection,
		WorkerService:          workerService,
	}

	return config
}

// NewService creates a new configured CLG service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.ConnectionService == nil {
		return nil, maskAnyf(invalidConfigError, "connection service must not be empty")
	}
	if config.EventCollection == nil {
		return nil, maskAnyf(invalidConfigError, "event collection must not be empty")
	}
	if config.InstrumentorCollection == nil {
		return nil, maskAnyf(invalidConfigError, "instrumentor collection must not be empty")
	}
	if config.PeerCollection == nil {
		return nil, maskAnyf(invalidConfigError, "peer collection must not be empty")
	}
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}
	if config.WorkerService == nil {
		return nil, maskAnyf(invalidConfigError, "worker service must not be empty")
	}

	newService := &service{
		// Dependencies.
		connection:   config.ConnectionService,
		event:        config.EventCollection,
		instrumentor: config.InstrumentorCollection,
		peer:         config.PeerCollection,
		storage:      config.StorageCollection,
		worker:       config.WorkerService,

		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		shutdownOnce: sync.Once{},
	}

	return newService, nil
}

type service struct {
	// Dependencies.
	connection   connection.Service
	event        *event.Collection
	instrumentor *instrumentor.Collection
	peer         *peer.Collection
	storage      *storage.Collection
	worker       worker.Service

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	shutdownOnce sync.Once
}

func (s *service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *service) CLGCount(ctx context.Context, args []reflect.Value) error {
	counterConfig := s.instrumentor.Publisher.CounterConfig()
	counterConfig.SetHelp("Number of CLGs being executed.")
	counterConfig.SetName(s.instrumentor.Publisher.NewKey("clgs", "total"))
	counter, err := s.instrumentor.Publisher.Counter(counterConfig)
	if err != nil {
		return maskAny(err)
	}
	counter.Increment(1)

	return nil
}

func (s *service) CLGCountPerCLGTree(ctx context.Context, args []reflect.Value) error {
	instrumentorKey := s.instrumentor.Publisher.NewKey("clgs", "per", "clg", "tree", "total")

	// We want to count CLGs being executed per CLG tree. To do so we have to
	// increment a counter for each CLG tree that is currently being executed.
	// This intermediate counter is stored in the instrumentor storage to be able
	// to increment the counter accross multiple processes. Once we know the final
	// result of our metric, we can emit it to the actual instrumentor.
	clgTreeID, ok := clgtreeid.FromContext(ctx)
	if !ok {
		return maskAnyf(invalidContextError, "clg tree id must not be empty")
	}
	storageKey := fmt.Sprintf("%s:%s", instrumentorKey, clgTreeID)
	counter, err := s.storage.Instrumentor.Increment(storageKey, 1)
	if err != nil {
		return maskAny(err)
	}

	// We only want to emit the actual result when there is no activity around the
	// current CLG tree ID anymore. Our queue manages labeled signal events. So,
	// in case the queue does not hold any event labeled with the current CLG tree
	// ID anymore, we can emit the actual metric.
	ok, err = s.event.Signal.ExistsAnyWithLabel(clgTreeID)
	if err != nil {
		return maskAny(err)
	}
	if ok {
		// There do still signal events exist being labeled with the current CLG
		// tree ID. Therefore we cannot emit our actual metric yet.
		return nil
	}

	// Here we want to emit our actual metric. In case this code runs the very
	// first time, we are creating the metric within the instrumentor, otherwise
	// we reuse it to emit the actual counter.
	gaugeConfig := s.instrumentor.Publisher.GaugeConfig()
	gaugeConfig.SetHelp("Number of CLGs being executed per CLG tree.")
	gaugeConfig.SetName(instrumentorKey)
	gauge, err := s.instrumentor.Publisher.Gauge(gaugeConfig)
	if err != nil {
		return maskAny(err)
	}
	gauge.Set(counter)

	// At this point we definitely know that there is no more event activity
	// associated with the current CLG tree ID. We emitted our actual metric and
	// therefore we can simply remove our intermediate counter.
	err = s.storage.Instrumentor.Remove(storageKey)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) CLGID(ctx context.Context, args []reflect.Value) error {
	destinationID, ok := destinationid.FromContext(ctx)
	if !ok {
		return maskAnyf(invalidContextError, "destination id must not be empty")
	}
	sourceIDs, ok := sourceids.FromContext(ctx)
	if !ok {
		return maskAnyf(invalidContextError, "source ids must not be empty")
	}

	// Prepare a queue to synchronise the workload.
	queue := make(chan string, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		queue <- sourceID
	}

	peerB, err := s.peer.Behaviour.Create(destinationID)
	if err != nil {
		return maskAny(err)
	}

	// Define the action being executed by the worker service. This action is
	// supposed to be executed concurrently. Therefore the queue we just created
	// is used to synchronize the workload.
	actions := []func(canceler <-chan struct{}) error{
		func(canceler <-chan struct{}) error {
			sourceID := <-queue

			peerA, err := s.peer.Behaviour.Create(sourceID)
			if err != nil {
				return maskAny(err)
			}

			// Connect source and destination ID of the CLG in the behaviour layer of
			// the connection space.
			_, err = s.connection.Create(peerA.Kind(), peerB.Kind(), peerA.ID(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	executeConfig := s.worker.ExecuteConfig()
	executeConfig.Actions = actions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(sourceIDs)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) CLGName(ctx context.Context, args []reflect.Value) error {
	destinationName, ok := destinationname.FromContext(ctx)
	if !ok {
		return maskAnyf(invalidContextError, "destination name must not be empty")
	}
	sourceNames, ok := sourcenames.FromContext(ctx)
	if !ok {
		return maskAnyf(invalidContextError, "source names must not be empty")
	}

	// Prepare a queue to synchronise the workload.
	queue := make(chan string, len(sourceNames))
	for _, n := range sourceNames {
		queue <- n
	}

	peerB, err := s.peer.Behaviour.Create(destinationName)
	if err != nil {
		return maskAny(err)
	}

	// Define the action being executed by the worker service. This action is
	// supposed to be executed concurrently. Therefore the queue we just created
	// is used to synchronize the workload.
	actions := []func(canceler <-chan struct{}) error{
		func(canceler <-chan struct{}) error {
			sourceName := <-queue

			peerA, err := s.peer.Behaviour.Create(sourceName)
			if err != nil {
				return maskAny(err)
			}

			// Connect source and destination name of the CLG in the behaviour layer
			// of the connection space.
			_, err = s.connection.Create(peerA.Kind(), peerB.Kind(), peerA.ID(), peerB.ID())
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	executeConfig := s.worker.ExecuteConfig()
	executeConfig.Actions = actions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(sourceNames)
	err = s.worker.Execute(executeConfig)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *service) Track(ctx context.Context, args []reflect.Value) error {
	actions := []func(canceler <-chan struct{}) error{
		func(canceler <-chan struct{}) error {
			err := s.CLGCount(ctx, args)
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		func(canceler <-chan struct{}) error {
			err := s.CLGCountPerCLGTree(ctx, args)
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		func(canceler <-chan struct{}) error {
			err := s.CLGID(ctx, args)
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
		func(canceler <-chan struct{}) error {
			err := s.CLGName(ctx, args)
			if err != nil {
				return maskAny(err)
			}

			return nil
		},
	}

	executeConfig := s.worker.ExecuteConfig()
	executeConfig.Actions = actions
	executeConfig.Canceler = s.closer
	executeConfig.NumWorkers = len(actions)
	err := s.worker.Execute(executeConfig)
	if err != nil {
		return maskAny(err)
	}

	return nil
}
