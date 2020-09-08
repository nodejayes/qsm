package connection

import (
	"database/sql"
	"errors"
)

// New create a new instance of Connection and returns the reference to it
func New(connectionString string, typ string) *Connection {
	return &Connection{
		connectionString: connectionString,
		instance:         nil,
		typ:              typ,
		err:              nil,
	}
}

// Connection the interface to the underlying database
type Connection struct {
	connectionString string
	instance         *sql.DB
	typ              string
	err              error
}

// GetInstance the getter for the sql.DB instance
func (ctx *Connection) GetInstance() *sql.DB {
	return ctx.instance
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
	ctx.instance, ctx.err = sql.Open(ctx.typ, ctx.connectionString)
	if ctx.err != nil {
		return
	}
	ctx.instance.SetMaxOpenConns(maxOpen)
}

// Disconnect close the Connection to the underlying database
func (ctx *Connection) Disconnect() {
	if !ctx.IsConnected() {
		return
	}
	ctx.err = ctx.instance.Close()
}

// IsConnected check if the current Connection to the underlying database is open and active
func (ctx *Connection) IsConnected() bool {
	if ctx.instance == nil {
		return false
	}
	pingResult := ctx.instance.Ping()
	if pingResult == nil {
		return true
	}
	ctx.err = errors.New(pingResult.Error())
	return false
}
