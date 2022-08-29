# pgproxy

A library for making postgresql server proxies, handle message flow and redirect them to a true postgresql server.

[Example of implementation here](https://github.com/Diskyver/pgproxy-example)


## Installation

```
go get -v https://github.com/Diskyver/pgproxy
```

## Usage

Call the `CreatePgProxy` function in order to create a new proxy. You need to implement the PgProxySession interface yourself to define the wanted behavior during the session.

See the godoc for more details