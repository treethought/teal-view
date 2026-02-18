package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/treethought/teal-view/at"
	"github.com/treethought/teal-view/db"
	"github.com/treethought/teal-view/ui"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	logFile, err := os.Create("teal-view.log")
	if err != nil {
		panic(fmt.Sprintf("failed to create log file: %v", err))
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: true,
	})


	gdb, err := db.NewDB()
	if err != nil {
		panic(err)
	}
	store := db.NewStore(gdb)
	consumer := at.NewConsumer(store)

	ctx := context.Background()

	sig := make(chan os.Signal, 1)
	errCh := make(chan error, 1)

	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	go func() {
		errCh <- consumer.Start(ctx)
	}()

	app := ui.NewApp(store, consumer)

	p := tea.NewProgram(app, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("UI error: %v\n", err)
	}

	select {
	case <-ctx.Done():
		fmt.Println("Shutting down consumer...")
	case err := <-errCh:
		fmt.Printf("Consumer error: %v\n", err)
	case sig := <-sig:
		fmt.Printf("Received signal: %v. Shutting down consumer...\n", sig)
	}

}
