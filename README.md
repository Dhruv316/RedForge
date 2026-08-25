# RedForge

A Redis-like in-memory data store implemented from scratch in Java, featuring the RESP protocol, TCP client-server communication, transactions, key expiration, and master-replica replication. Includes Docker containerization and Kubernetes deployment configurations using Kind and Helm.

## Overview

RedForge is a Java-based Redis implementation built for learning and experimentation with systems programming concepts. It communicates over TCP using the Redis Serialization Protocol (RESP) and provides an in-memory key-value store with transaction and replication support.

## Core capabilities

- RESP request parsing and response serialization
- TCP client-server communication
- `PING`, `ECHO`, `SET`, `GET`, and `INCR`
- Key expiration with `SET ... PX`
- Transactions with `MULTI`, `EXEC`, and `DISCARD`
- Master-replica replication with `PSYNC`, `REPLCONF`, and `WAIT`
- Concurrent client handling
- Docker containerization
- Kubernetes deployment configuration with Kind and Helm

## Run with Docker

```bash
docker build -t redforge .
docker run -p 6379:6379 --name redforge redforge
```

The server listens on port `6379` by default.

## Tech stack

Java 23 · Maven · Spring Framework · TCP/IP · RESP · Docker · Kubernetes · Helm · Kind
