# pgproxy

[![Go](https://github.com/Diskyver/pgproxy/actions/workflows/go.yml/badge.svg)](https://github.com/Diskyver/pgproxy/actions/workflows/go.yml)

A library for making postgresql server proxies, handle message flow and redirect them to a true postgresql server.

[Example of implementation here](https://github.com/Diskyver/pgproxy-example)

## Installation

```
go get -v https://github.com/Diskyver/pgproxy
```

## Usage

Call the `CreatePgProxy` function in order to create a new proxy. You need to implement the PgProxySession interface yourself to define the wanted behavior during the session.

```go
type Session struct{}

func (s *Session) OnConnect(_ *pgx.Conn) {
	fmt.Println("I'm in!")
}

func (s *Session) OnQuery(query *pgproto3.Query) (*pgproto3.Query, error) {
	fmt.Println("query:", query.String)
	return query, nil
}

func (s *Session) OnResult(rows pgx.Rows, err error) {
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("%w", rows)
}

func (s *Session) OnClose(_ *pgx.Conn) {
	fmt.Println("I'm out")
}

func main() {
	proxy := pgproxy.CreatePgProxy("postgres://user:password@localhost:5432", &Session{})
	proxy.Listen("localhost:8080")
}
```

See the godoc for more details

```
$ go doc -all
package pgproxy // import "github.com/diskyver/pgproxy"


FUNCTIONS

func ReadyForQueryMesage() []byte

TYPES

type PgProxyServer struct {
	// Has unexported fields.
}
    A PgProxyServer is a postgresql server proxy it has a pool of connection to
    a true postgresql server

func CreatePgProxy(pgUri string, session PgProxySession) *PgProxyServer
    CreatePgProxy create a new proxy for a postgresql server Allows to redirect
    queries to a true postgresql server pgUri describe the postgresql URI for
    the postgresql server. See
    https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

func CreatePgServer(pgUri string, session PgProxySession) *PgProxyServer
    CreatePgServer create a new proxy for a postgresql server without a
    pgxpool.Pool pgUri describe the postgresql URI for the postgresql server.
    See
    https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

func (p *PgProxyServer) Listen(addr string) error
    Listen TCP packets that use Message Flow postgresql protocol create also a
    connection pool to a postgresql server

type PgProxySession interface {
	// OnConnect handle the postgresql client socket on established connection
	OnConnect(ctx context.Context, socket *pgx.Conn) error
	// OnQuery handle the query before the postgresql server
	// you can edit the query here or simply return an error if
	// you don't want to send the query to the postgresql server.
	OnQuery(query *pgproto3.Query) (*pgproto3.Query, error)
	// OnResult handle the query's result, err is define if something
	// wrong occured from the postgresql server.
	OnResult(rows pgx.Rows, err error)
	// OnClose handle the postgresql client socket before to close the connection
	OnClose(socket *pgx.Conn) bool
}
    Define the behavior you want during the session by implementing the
    PgProxySession interface

type PgServer struct {
	// Has unexported fields.
}
    A PgServer is a postgresql server proxy

func (p *PgServer) Listen(addr string) error
    Listen TCP packets that use Message Flow postgresql protocol

type PgServerSession interface {
	// OnQuery handle the query before the postgresql server
	// you can edit the query here or simply return an error if
	// you don't want to send the query to the postgresql server.
	OnQuery(query *pgproto3.Query) ([]byte, error)
}
    Define the behavior you want during the session by implementing the
    PgServerSession interface
```