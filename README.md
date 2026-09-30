# Emergency Messaging Platform

**Multithreaded C++ client · Concurrent Java server · TCP / STOMP-style messaging**

An academic client–server messaging application for sharing emergency-event reports across topic-based channels. Users connect to a central server, subscribe to channels (such as `firefighters` or `police`), publish event reports, and receive reports sent to the same channels.

The project focuses on **socket programming, concurrency, message framing, subscription management, and two alternative server concurrency models**. It is an educational prototype, **not** a deployed emergency-response system or a hard real-time application.

## Architecture at a glance

```text
  C++ client A                 Java server                  C++ client B
  ┌─────────────────┐         ┌──────────────────────┐     ┌─────────────────┐
  │ User input      │── TCP ──│ STOMP-style protocol │──TCP│ User input      │
  │ Network receive │◄────────│ Topic subscriptions  │────►│ Network receive │
  │ Event summaries │         │ Message routing      │     │ Event summaries │
  └─────────────────┘         └──────────────────────┘     └─────────────────┘
                                    │
                           Select server model:
                      Thread-per-client | Reactor
```

- **C++17 client:** reads terminal commands, connects to the server through Boost.Asio TCP sockets, builds/parses protocol frames, processes events, and generates local summaries.
- **Java server:** accepts connections, handles connection and subscription requests, and routes published reports to subscribers of each destination.
- **Wire format:** a STOMP-inspired, text-based application protocol on top of TCP. The code includes version `1.2` in connection frames, but full standards compliance has not been independently verified.

## Features

- User login and session handling through `CONNECT` / `CONNECTED` frames.
- Subscribe to, and leave, named channels.
- Publish structured event reports from JSON input files.
- Receive reports published by other clients subscribed to the same channel.
- Maintain a local collection of event reports and write a **plain-text** summary for a selected channel and reporting user, including event counts and selected event details.
- Return receipt and error responses for supported protocol operations.
- Run the server in **thread-per-client (TPC)** or **Reactor** mode.

## Concurrency design

### C++ client

The client separates two potentially blocking activities into two execution paths:

1. A **keyboard thread** reads commands and sends requests.
2. The **main thread** receives and processes frames from the server.

This prevents waiting for terminal input from also preventing the application from handling incoming network data. The client uses `std::thread`, `std::mutex`, and atomic variables in parts of its shared-state management. Its Boost.Asio socket reads and writes are **blocking**; concurrent input and reception come from separate threads, not from Boost.Asio async APIs.

### Java server

The same application protocol can be served using either model:

| Mode | Design | Trade-off |
| --- | --- | --- |
| **TPC** | A dedicated thread handles each client connection. | Straightforward model, with thread overhead that grows with connected clients. |
| **Reactor** | Java NIO selector-based non-blocking connection handling, with a worker thread pool for processing. | Separates connection readiness from processing and reduces the need for one dedicated thread per connection. |

The Reactor's task dispatch also coordinates work associated with individual connection handlers. The two modes are **alternatives**, not two modes running simultaneously in one server instance.

## Technology stack

| Component | Technology |
| --- | --- |
| Client | C++17, Boost.Asio, C++ standard threading/synchronization primitives |
| Server | Java 8 language target, Java NIO, Java concurrency utilities |
| Transport and framing | TCP sockets, STOMP-style null-terminated frames |
| Data exchange / reporting | JSON event inputs, local plain-text summaries |
| Build tools | GNU Make / g++, Maven |

## Repository layout

```text
.
├── client/
│   ├── src/                  # CLI, protocol, TCP connection, event handling
│   ├── include/              # C++ headers and bundled JSON header
│   ├── data/                 # Sample event JSON files
│   └── makefile
├── server/
│   ├── src/main/java/bgu/spl/net/
│   │   ├── impl/stomp/       # STOMP message parsing and request handling
│   │   └── srv/              # Connection handlers, TPC, Reactor, routing
│   └── pom.xml
└── README.md
```

The `server` directory also contains supporting networking exercises in other packages; the emergency messaging entry point is `bgu.spl.net.impl.stomp.StompServer`.

## Build and run

**Prerequisites:** JDK 8+ and Maven; a C++17-capable g++, GNU Make, Boost.Asio / Boost.System development libraries, and pthread support. Commands below assume a Unix-like development environment.

**1. Build and start the server:**

```bash
cd server
mvn compile
java -cp target/classes bgu.spl.net.impl.stomp.StompServer 7777 reactor
# Or replace "reactor" with "tpc" for thread-per-client mode.
```

**2. In a different terminal, build and start a client:**

```bash
cd client
make
./bin/StompEMIClient
```

**3. Example interactive commands:**

```text
login 127.0.0.1:7777 alice password123
join police
report data/events1.json
summary police alice police-summary.txt
exit police
logout
```

Use a second client in another terminal, log in with a different username, and join the same channel to observe message fan-out. Sample files in `client/data/` use channel names defined in their JSON content: make sure the clients subscribe to the corresponding channel. The `summary` command creates a local text file from events held by that client; it does **not** export all server-side history.

> **Build/runtime note:** These commands follow the repository's entry points and build configurations. End-to-end operation across environments and all command sequences has not been verified here.

## What this project demonstrates

- Practical client–server design across **two languages**.
- How two blocking I/O activities can be handled concurrently using separate execution threads.
- Topic-based publish/subscribe routing and framing messages on a TCP byte stream.
- Differences between **thread-per-client** and **selector-based Reactor** server architectures.
- Managing shared data and reasoning about synchronization, error handling, and connection lifecycles.

## Limitations and possible improvements

This repository reflects an academic implementation, not a production-hardened service. Areas for future work include:

- Audit and standardize synchronization across **all** access to shared client state.
- Implement coordinated client shutdown and predictable cleanup of blocking operations and threads.
- Review unsubscribe-by-ID handling and validate channel/subscription mappings.
- Expand tests for disconnected peers, malformed frames, concurrent subscriptions, and reconnects.
- Verify the supported feature subset against the complete STOMP 1.2 specification.

**Terminology:** “Real-time messaging” here means connected users can receive updates as they arrive; the system does **not** guarantee hard real-time deadlines.
