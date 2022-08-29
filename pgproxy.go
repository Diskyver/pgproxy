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
)

// Define the behavior you want during the session
// by implementing the PgProxySession interface
type PgProxySession interface {
	OnConnect(socket *pgx.Conn)
	OnQuery(query *pgproto3.Query) (*pgproto3.Query, error)
	OnResult(rows pgx.Rows, err error)
	OnClose(socket *pgx.Conn)
}

type pgProxyServerBackend struct {
	backend *pgproto3.Backend
	conn    net.Conn
	db      *pgx.Conn
}

func newPgProxyServerBackend(conn net.Conn, db *pgx.Conn) *pgProxyServerBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return &pgProxyServerBackend{
		backend: backend,
		conn:    conn,
		db:      db,
	}
}

func (p *pgProxyServerBackend) runQueryOnDB(query *pgproto3.Query) (pgx.Rows, error) {
	row, err := p.db.Query(context.Background(), query.String)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query row failed: %v\n", err)
		return nil, err
	}
	fmt.Println(row)
	return row, nil
}

func (p *pgProxyServerBackend) run(session PgProxySession) error {
	defer func() {
		session.OnClose(p.db)
		p.close()
	}()

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

func (p *pgProxyServerBackend) close() error {
	if p != nil && p.db != nil && !p.db.IsClosed() {
		if err := p.db.Close(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "Unable to close gracefully the database connection: %e", err)
			return err
		}
	}

	if p != nil && p.conn != nil {
		if err := p.conn.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Unable to close gracefully the tcp server %e", err)
			return err
		}
	}

	return nil
}

// A PgProxyServer is a postgresql server proxy
type PgProxyServer struct {
	pgUri   string
	session PgProxySession
	backend *pgProxyServerBackend
}

// Listen TCP packets that use Message Flow postgresql protocol
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

		db, err := pgx.Connect(context.Background(), p.pgUri)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to reach the database %s", err)
		}

		p.session.OnConnect(db)

		p.backend = newPgProxyServerBackend(conn, db)

		sig_chan := make(chan os.Signal)
		signal.Notify(sig_chan, os.Interrupt, syscall.SIGTERM)

		go func() {
			err := p.backend.run(p.session)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Something wrong occured with the backend %s", err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}

// CreatePgProxy create a new proxy for a postgresql server
// pgUri describe the postgresql URI for the postgresql server. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
func CreatePgProxy(pgUri string, session PgProxySession) *PgProxyServer {
	return &PgProxyServer{pgUri: pgUri, session: session, backend: nil}
}

// Close the PgProxyServer, close the database connection and the tcp server
func (p *PgProxyServer) Close() error {
	return p.backend.close()
}
