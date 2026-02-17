package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/treethought/teal-view/db"
)

func main() {

	gdb, err := db.NewDB()
	if err != nil {
		panic(err)
	}
	store := db.NewStore(gdb)
	consumer := NewConsumer(store)

	ctx := context.Background()

	sig := make(chan os.Signal, 1)
	errCh := make(chan error, 1)

	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		errCh <- consumer.Start(ctx)
	}()

	select {
	case <-ctx.Done():
		fmt.Println("Shutting down consumer...")
	case err := <-errCh:
		fmt.Printf("Consumer error: %v\n", err)
	case sig := <-sig:
		fmt.Printf("Received signal: %v. Shutting down consumer...\n", sig)
	}

}
