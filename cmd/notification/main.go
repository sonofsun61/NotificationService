package main

import (
	"NotificationService/internal/app"
	"log/slog"
	"os"
)

func main() {
	app := app.InitializeApp()
	if err := app.Run(); err != nil {
		slog.Error("app stopped with error", "err", err)
		os.Exit(1)
	}
}
