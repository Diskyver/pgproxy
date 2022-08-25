package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v4"
)

var options struct {
	proxyAddr string
	pgAddr    string
}

func pgAddr() (string, error) {
	if addr := os.Getenv("PGPROXY_DB_URL"); addr != "" {
		return addr, nil
	}

	if addr := options.pgAddr; addr != "" {
		return addr, nil
	}

	return "", errors.New("postgresql url not provided")
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.proxyAddr, "proxy-addr", "127.0.0.1:15432", "Proxy address")
	flag.StringVar(&options.pgAddr, "pg-addr", "127.0.0.1:5432", "Postgresql url. Also the PGPROXY_DB_URL environment variable exists")
	flag.Parse()

	ln, err := net.Listen("tcp", options.proxyAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		db_url, err := pgAddr()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			os.Exit(1)
		}

		db, err := pgx.Connect(context.Background(), db_url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to reach the database %s", err)
			os.Exit(1)
		}

		b := NewPgProxyServer(conn, db)
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
