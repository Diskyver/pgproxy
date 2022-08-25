# pgproxy WIP

A proxy for postgresql server, handle message flow and redirect them to a true postgresql server.

This project is based on this example https://github.com/jackc/pgproto3/tree/master/example/pgfortune. This project's goal is to be a standalone program with a cli (currently that's the case) and also a library for making custom proxies by yourself using a friendly API.

## Installation

```
go get ./...
```

Try it from the source code.

```
go run *.go -h
```

## Usage

```
$ pgproxy -h
```