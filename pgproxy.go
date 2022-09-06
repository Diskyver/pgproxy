package pgproxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Define the behavior you want during the session
// by implementing the PgProxySession interface
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

type pgProxyServerBackend struct {
	backend *pgproto3.Backend
	conn    net.Conn
	pool    *pgxpool.Pool
}

func newPgProxyServerBackend(conn net.Conn, pool *pgxpool.Pool) *pgProxyServerBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return &pgProxyServerBackend{
		backend: backend,
		conn:    conn,
		pool:    pool,
	}
}

func (p *pgProxyServerBackend) runQueryOnDB(query *pgproto3.Query) (pgx.Rows, error) {
	row, err := p.pool.Query(context.Background(), query.String)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query row failed: %v\n", err)
		return nil, err
	}
	fmt.Println(row)
	return row, nil
}

func (p *pgProxyServerBackend) run(session PgProxySession) error {
	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error receiving message: %s", err)
			return err
		}

		switch msg.(type) {
		case *pgproto3.Query:
			query, is_casted := msg.(*pgproto3.Query)
			if !is_casted {
				fmt.Fprintf(os.Stderr, "Unable to cast FrontendMessage into Query concrete type")
			}

			query, err := session.OnQuery(query)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %s", err)
				return err
			}

			rows, err := p.runQueryOnDB(query)
			session.OnResult(rows, err)

			if err != nil {
				buf := (&pgproto3.ErrorResponse{
					Message: string(err.Error()),
				}).Encode(nil)
				buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
				_, err = p.conn.Write(buf)
				break
			}

			fields := rows.FieldDescriptions()
			fmt.Println(fields)
			buf := (&pgproto3.RowDescription{
				Fields: fields,
			}).Encode(nil)

			buf = (&pgproto3.DataRow{Values: rows.RawValues()}).Encode(buf)
			buf = (&pgproto3.CommandComplete{CommandTag: []byte(query.String)}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			_, err = p.conn.Write(buf)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error writing query response: %s", err)
				return err
			}
		case *pgproto3.Terminate:
			return nil
		default:
			fmt.Fprintf(os.Stderr, "received message other than Query from client: %#v", msg)
			return nil
		}

		return nil
	}
}

func (p *pgProxyServerBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error receiving startup message: %s", err)
		return err
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error sending ready for query: %s", err)
			return err
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error sending deny SSL request: %s", err)
			return err
		}
		return p.handleStartup()
	default:
		fmt.Fprintf(os.Stderr, "unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *pgProxyServerBackend) errorResponse(msg string) error {
	buf := (&pgproto3.ErrorResponse{Message: msg}).Encode(nil)
	_, err := p.conn.Write(buf)
	return err
}

// A PgProxyServer is a postgresql server proxy
// it has a pool of connection to a true postgresql server
type PgProxyServer struct {
	pgUri   string
	session PgProxySession
	backend *pgProxyServerBackend
}

// Listen TCP packets that use Message Flow postgresql protocol
// create also a connection pool to a postgresql server
func (p *PgProxyServer) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return err
	}

	log.Println("Listening on", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())

		config, err := pgxpool.ParseConfig(p.pgUri)
		if err != nil {
			log.Fatal(err)
		}

		config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			return p.session.OnConnect(ctx, conn)
		}

		config.AfterRelease = func(conn *pgx.Conn) bool {
			return p.session.OnClose(conn)
		}

		pool, err := pgxpool.ConnectConfig(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}

		defer func(conn net.Conn, pool *pgxpool.Pool) {
			conn.Close()
			if pool != nil {
				pool.Close()
			}
		}(conn, pool)

		p.backend = newPgProxyServerBackend(conn, pool)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to reach the database: %s", err)
			p.backend.errorResponse(err.Error())
		} else {
			sig_chan := make(chan os.Signal)
			signal.Notify(sig_chan, os.Interrupt, syscall.SIGTERM)

			backend_channel := make(chan error)

			go func(c chan error) {
				err := p.backend.run(p.session)
				c <- err
			}(backend_channel)

			err := <-backend_channel
			if err != nil {
				fmt.Fprintf(os.Stderr, "Something wrong occured with the backend %s", err)
			}

			log.Println("Closed connection from", conn.RemoteAddr())
		}

	}
}

// CreatePgProxy create a new proxy for a postgresql server
// Allows to redirect queries to a true postgresql server
// pgUri describe the postgresql URI for the postgresql server. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
func CreatePgProxy(pgUri string, session PgProxySession) *PgProxyServer {
	return &PgProxyServer{pgUri: pgUri, session: session, backend: nil}
}

// PgServerSession define the behavior you want during the session
// by implementing the PgServerSession interface
type PgServerSession interface {
	// OnQuery handle the query before the postgresql server
	// you can edit the query here or simply return an error if
	// you don't want to send the query to the postgresql server.
	OnQuery(query *pgproto3.Query) ([]byte, error)
}

type pgServerBackend struct {
	backend *pgproto3.Backend
	conn    net.Conn
}

func newPgServerBackend(conn net.Conn) *pgProxyServerBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return &pgProxyServerBackend{
		backend: backend,
		conn:    conn,
	}
}

func ReadyForQueryMesage() []byte {
	return (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)
}

func (p *pgServerBackend) run(session PgServerSession) error {
	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error receiving message: %s", err)
			return err
		}

		switch msg.(type) {
		case *pgproto3.Query:
			query, is_casted := msg.(*pgproto3.Query)
			if !is_casted {
				fmt.Fprintf(os.Stderr, "Unable to cast FrontendMessage into Query concrete type")
			}

			reply_msg, err := session.OnQuery(query)
			if err != nil {
				buf := (&pgproto3.ErrorResponse{
					Message: string(err.Error()),
				}).Encode(nil)
				buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
				_, err = p.conn.Write(buf)
				break
			}

			_, err = p.conn.Write(reply_msg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error writing query response: %s", err)
				return err
			}
		case *pgproto3.Terminate:
			return nil
		default:
			fmt.Fprintf(os.Stderr, "received message other than Query from client: %#v", msg)
			return nil
		}

		return nil
	}
}

func (p *pgServerBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error receiving startup message: %s", err)
		return err
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error sending ready for query: %s", err)
			return err
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error sending deny SSL request: %s", err)
			return err
		}
		return p.handleStartup()
	default:
		fmt.Fprintf(os.Stderr, "unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *pgServerBackend) errorResponse(msg string) error {
	buf := (&pgproto3.ErrorResponse{Message: msg}).Encode(nil)
	_, err := p.conn.Write(buf)
	return err
}

// A PgServer is a postgresql server proxy
type PgServer struct {
	pgUri   string
	session PgProxySession
	backend *pgProxyServerBackend
}

// Listen TCP packets that use Message Flow postgresql protocol
func (p *PgServer) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return err
	}

	log.Println("Listening on", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())

		p.backend = newPgServerBackend(conn)

		sig_chan := make(chan os.Signal)
		signal.Notify(sig_chan, os.Interrupt, syscall.SIGTERM)

		backend_channel := make(chan error)

		go func(c chan error) {
			err := p.backend.run(p.session)
			c <- err
		}(backend_channel)

		err_channel := <-backend_channel
		if err_channel != nil {
			fmt.Fprintf(os.Stderr, "Something wrong occured with the backend %s", err)
		}

		log.Println("Closed connection from", conn.RemoteAddr())

	}
}

// CreatePgServer create a new proxy for a postgresql server without a pgxpool.Pool
// pgUri describe the postgresql URI for the postgresql server. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
func CreatePgServer(pgUri string, session PgProxySession) *PgProxyServer {
	return &PgProxyServer{pgUri: pgUri, session: session, backend: nil}
}
