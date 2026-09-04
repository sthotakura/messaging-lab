# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Build: `dotnet build messaging-lab.slnx` (or `dotnet build` from within `messaging-lab.solace.fw/`)
- Target framework: `net10.0`, nullable reference types and implicit usings are both enabled.
- There are no test projects yet.

## Architecture

This is a Solace PubSub+ messaging library (`messaging-lab.solace.fw`, root namespace `messaging_lab.solace.fw`), built as a small ports-and-adapters layer over the native `SolaceSystems.Solclient.Messaging` SDK (NuGet package, pinned in the csproj).

The library is split into three transport-agnostic interface namespaces (the "ports") plus a set of Solace-specific implementations (the "adapters") at the project root and alongside each namespace. None of the interfaces below reference any Solace type — only the `Solace*` classes know about `SolaceSystems.Solclient.Messaging`. Keep that boundary when adding new interfaces or adapters.

- **`serialization/`** — `IMessageSerializer<in T>` / `IMessageDeserializer<out T>`, implemented by `JsonMessageSerializer<T>` / `JsonMessageDeserializer<T>` (`System.Text.Json`).
- **`publish/`** — `IMessageSender<in T>` (low-level: send a pre-built domain object to a fixed destination) and `IMessagePublisher<in T>` (high-level facade, resolves the topic from `IMessagePublisherSettings.Topic`). `SolaceMessageSender<T>` serializes to JSON and publishes it as the binary attachment of a Solace message; `SolacePublisher<T>` wraps a sender and owns/disposes it via `IDisposable` when it created the sender itself.
- **`subscribe/`** — `IMessageHandler<in T>` (business logic, returns `bool`) and `IMessageSubscriber` (`Subscribe()`/`Unsubscribe()` lifecycle). `SolaceSubscriber<T>` binds a client-acknowledged, guaranteed-delivery `IFlow` to the queue named in `IMessageSubscriberSettings.Queue`.

Connection primitives live at the project root, not under `publish`/`subscribe`, since both share them:

- `SolaceMessagingEnvironment` — thread-safe, once-only guard around the process-wide `ContextFactory.Init`/`Cleanup` calls the native SDK requires.
- `SolaceContext` — wraps `IContext`; ensures the environment is initialized and is the factory for sessions.
- `SolaceSession` — wraps `ISession`; re-exposes the message/session event delegates (which must be supplied at `CreateSession` time) as ordinary, subscribable-after-construction .NET events (`MessageReceived`, `SessionEventOccurred`).

**Concurrency note:** a Solace `IContext` drives all I/O and delivery callbacks from a single owned thread, and the native SDK's own guidance is that callbacks must never block. `SolaceSubscriber<T>` respects this: its flow's message callback only writes the received `IMessage` into a bounded `Channel<IMessage>` (capacity = the flow's `WindowSize`) and returns immediately; a pool of worker tasks (`concurrency`, default 4) drains the channel, doing the actual deserialize + handle + ack work. Each worker is supervised — if handling a message throws, the worker restarts itself and keeps draining rather than permanently losing that unit of concurrency. `Handle` returning `true` acks the message; `false` (or an exception) leaves it unacked for redelivery.

**Disposal ownership:** Solace destinations (`ITopic`/`IQueue`) and flows are `IDisposable`. Whoever creates one owns disposing it — `SolaceMessageSender<T>` disposes the destination it was given, `SolaceSubscriber<T>` disposes both the flow and the queue it created, and `SolacePublisher<T>` disposes its underlying sender if that sender is itself `IDisposable`. Preserve this ownership chain when extending the adapters.
