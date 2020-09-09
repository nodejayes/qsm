package connection

import (
	"database/sql"
	"errors"
	"os"
)

var connection *sql.DB

// New create a new instance of Connection and returns the reference to it
func New(connectionString string, typ string) *Connection {
	return &Connection{
		connectionString: os.Getenv(connectionString),
		typ:              typ,
		err:              nil,
	}
}

// Connection the interface to the underlying database
type Connection struct {
	connectionString string
	typ              string
	err              error
}

// GetInstance the getter for the sql.DB instance
func (ctx *Connection) GetInstance() *sql.DB {
	return connection
}

// GetLastError returns the last error of the Connection
func (ctx *Connection) GetLastError() error {
	return ctx.err
}

// Connect opens the Connection to the underlying database
//
// if the Connection already open, the Connection was closed and reopen
func (ctx *Connection) Connect(maxOpen int) {
	if ctx.IsConnected() {
		ctx.Disconnect()
	}
	connection, ctx.err = sql.Open(ctx.typ, ctx.connectionString)
	if ctx.err != nil {
		return
	}
	connection.SetMaxOpenConns(maxOpen)
}

// Disconnect close the Connection to the underlying database
func (ctx *Connection) Disconnect() {
	if !ctx.IsConnected() {
		return
	}
	ctx.err = connection.Close()
}

// IsConnected check if the current Connection to the underlying database is open and active
func (ctx *Connection) IsConnected() bool {
	if connection == nil {
		return false
	}
	pingResult := connection.Ping()
	if pingResult == nil {
		return true
	}
	ctx.err = errors.New(pingResult.Error())
	return false
}
