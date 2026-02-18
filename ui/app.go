package ui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/treethought/teal-view/at"
	"github.com/treethought/teal-view/db"
)

type App struct {
	store    *db.Store
	consumer *at.Consumer
	feed     *Feed
	active   tea.Model
}

func NewApp(store *db.Store, consumer *at.Consumer) *App {
	return &App{
		store:    store,
		consumer: consumer,
		feed:     NewFeed(store, consumer),
	}
}

func (a *App) Init() tea.Cmd {
	a.active = a.feed
	return a.feed.Init()
}

func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return a, tea.Quit
		}
	case tea.WindowSizeMsg:
		a.feed.SetSize(msg.Width, msg.Height)
		return a, nil
	}
	m, cmd := a.active.Update(msg)
	a.active = m
	return a, cmd
}

func (a *App) View() string {
	return a.active.View()
}
