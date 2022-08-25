package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

type PgProxyServer struct {
	backend *pgproto3.Backend
	conn    net.Conn
	db      *pgx.Conn
}

func NewPgProxyServer(conn net.Conn, db *pgx.Conn) *PgProxyServer {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	return &PgProxyServer{
		backend: backend,
		conn:    conn,
		db:      db,
	}
}

func (p *PgProxyServer) runQueryOnDB(query *pgproto3.Query) (pgx.Rows, error) {
	row, err := p.db.Query(context.Background(), query.String)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query row failed: %v\n", err)
		return nil, err
	}
	fmt.Println(row)
	return row, nil
}

func (p *PgProxyServer) Run() error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg.(type) {
		case *pgproto3.Query:
			query, is_casted := msg.(*pgproto3.Query)
			if !is_casted {
				fmt.Fprintf(os.Stderr, "Unable to cast FrontendMessage into Query concrete type")
			}

			if err != nil {
				return fmt.Errorf("error generating query response: %w", err)
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

			buf = (&pgproto3.DataRow{rows.RawValues()}).Encode(buf)
			buf = (&pgproto3.CommandComplete{CommandTag: []byte(query.String)}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			_, err = p.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (p *PgProxyServer) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *PgProxyServer) Close() error {
	p.db.Close(context.Background())
	return p.conn.Close()
}
