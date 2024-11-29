# watermill-badger
[![build](https://github.com/stevecallear/watermill-badger/actions/workflows/build.yml/badge.svg)](https://github.com/stevecallear/watermill-badger/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/stevecallear/watermill-badger/graph/badge.svg?token=3JBUN06BOD)](https://codecov.io/gh/stevecallear/watermill-badger)
[![Go Report Card](https://goreportcard.com/badge/github.com/stevecallear/watermill-badger)](https://goreportcard.com/report/github.com/stevecallear/watermill-badger)

`watermill-badger` provides a [Watermill](https://watermill.io/) pub/sub implementation backed by [BadgerDB](https://dgraph.io/docs/badger/). The implementation is similar to that of [`watermill-bolt`](https://github.com/ThreeDotsLabs/watermill-bolt), but with the addition of delayed publish, visibility timeout and the use of a registry to describe topic/subscription mapping. It is fundamentally a simplified version of [`emmq`](https://github.com/stevecallear/emmq) with the same goals, but leveraging the established patterns of Watermill.

## Getting Started
```
go get github.com/stevecallear/watermill-badger@latest
```
```
db, err := badgerdb.Open(badgerdb.DefaultOptions("./badger"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()

registry := badger.NewInMemoryRegistry(badger.InMemoryRegistryConfig{})

subscriber := badger.NewSubscriber(testDB, registry, badger.SubscriberConfig{})
defer subscriber.Close()

ch, err := subscriber.Subscribe(context.Background(), "topic")
if err != nil {
    log.Fatal(err)
}

publisher := badger.NewPublisher(testDB, registry, badger.PublisherConfig{})
defer publisher.Close()

publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), message.Payload("payload")))

msg := <-ch
fmt.Println(string(msg.Payload))
//output: payload
```

## Registry
The `Registry` is responsible for sequence generation and key prefix storage. `badger.NewInMemoryRegistry` returns an in-memory implementation that provides consistency on a per-instance basis. If persistence is required then `badger.NewPersistentRegistry` returns a DB-backed implementation with prefix persistence and global sequences. Due to Badger DB instances not being shareable across processes the `InMemoryRegistry` is typically sufficient for the majority of use cases.

## Message Delivery
Messages will generally be delivered to subscribers in FIFO order. Sequences are unique for the specific instance when using the `InMemoryRegistry`. It is therefore unlikely, but not impossible that two messages will have the same due time (nanosecond precision) and sequence. In this case ordering will be random. `PersistentRegistry` makes use of Badger DB sequences so will guarantee ordering assuming the sequence key remains stable.

## Visibility Timeout
The implementation adopts a visibility timeout model. This means that when a message is consumed it remains persisted with a configurable timeout value. Should the message be nacked, or the the process stopped during processing, then the message will be redelivered once the timeout period has elapsed.

## Publish Delay
The implementation supports delayed publish (this is how visibility timeout is implemented). As a result any use of the Watermill `delay` module will be honoured.
