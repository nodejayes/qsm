package connection

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/nodejayes/qsm/cfg"
)

func ExampleNew() {
	c := New("<Postgres Connection String>", "postgres")
	fmt.Printf(c.connectionString)
	// Output: <Postgres Connection String>
}

func ExampleConnection_Connect() {
	// not forget to import the pq package for postgres driver
	// _ "github.com/lib/pq"
	c := New(cfg.TestConnectionString, "postgres")
	// opens the connection with max 50 connections
	c.Connect(50)
	fmt.Printf("%v", c.IsConnected())
	// Output: true
	c.Disconnect()
}

func ExampleConnection_GetInstance() {
	c := New(cfg.TestConnectionString, "postgres")
	c.Connect(50)
	fmt.Printf("%v", c.GetInstance() != nil)
	// Output: true
	c.Disconnect()
}
