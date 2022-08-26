package pgproxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

type PgProxySession interface {
	OnConnect(socket *pgx.Conn)
	OnQuery(query *pgproto3.Query) (*pgproto3.Query, error)
}

type PgProxyServerBackend struct {
	backend *pgproto3.Backend
	conn    net.Conn
	db      *pgx.Conn
}

func NewPgProxyServerBackend(conn net.Conn, db *pgx.Conn) *PgProxyServerBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return &PgProxyServerBackend{
		backend: backend,
		conn:    conn,
		db:      db,
	}
}

func (p *PgProxyServerBackend) runQueryOnDB(query *pgproto3.Query) (pgx.Rows, error) {
	row, err := p.db.Query(context.Background(), query.String)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query row failed: %v\n", err)
		return nil, err
	}
	fmt.Println(row)
	return row, nil
}

func (p *PgProxyServerBackend) Run(session PgProxySession) error {
	defer p.Close()

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

func (p *PgProxyServerBackend) handleStartup() error {
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

func (p *PgProxyServerBackend) Close() error {
	p.db.Close(context.Background())
	return p.conn.Close()
}

type PgProxyServer struct {
	pgUrl   string
	session PgProxySession
}

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

		db, err := pgx.Connect(context.Background(), p.pgUrl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to reach the database %s", err)
		}

		p.session.OnConnect(db)

		b := NewPgProxyServerBackend(conn, db)
		go func() {
			err := b.Run(p.session)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Something wrong occured with the backend %s", err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}

func CreatePgProxy(pgUrl string, session PgProxySession) *PgProxyServer {
	return &PgProxyServer{pgUrl: pgUrl, session: session}
}
