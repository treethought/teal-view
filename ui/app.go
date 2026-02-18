package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	log "github.com/sirupsen/logrus"
	"github.com/treethought/teal-view/at"
	"github.com/treethought/teal-view/db"
)

type App struct {
	store    *db.Store
	consumer *at.Consumer
	events   <-chan *db.Play
	list     list.Model
}

func (a *App) waitForPlay() tea.Cmd {
	return func() tea.Msg {
		play := <-a.events
		return playMsg{play: play}
	}
}

type playsMsg struct {
	plays []*db.Play
}

type playMsg struct {
	play *db.Play
}

type playItem struct {
	play *db.Play
}

func (p playItem) Title() string {
	artists := strings.Builder{}
	for i, artist := range p.play.Artists {
		if i > 0 {
			artists.WriteString(", ")
		}
		artists.WriteString(artist.Name)
	}
	return fmt.Sprintf("%s - %s", p.play.TrackName, artists.String())
}
func (p playItem) Description() string {
	return fmt.Sprintf("@%s at %s", p.play.User.Handle, p.play.PlayedTime.Format("2006-01-02 15:04:05"))
}
func (p playItem) FilterValue() string { return p.play.UserDid }

func NewApp(store *db.Store, consumer *at.Consumer) *App {
	del := list.DefaultDelegate{
		ShowDescription: true,
		Styles:          list.NewDefaultItemStyles(),
	}
	del.SetHeight(2)

	l := list.New(nil, del, 80, 20)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(true)
	return &App{
		store:    store,
		consumer: consumer,
		list:     l,
	}
}

func (a *App) Init() tea.Cmd {
	a.events = a.consumer.SubPlays("*") // subscribe to plays for a specific DID
	cmd := func() tea.Msg {
		plays, err := a.store.ListRecentPlays(100)
		if err != nil {
			log.Errorf("Failed to load recent plays: %v", err)
			return nil
		}
		return playsMsg{plays: plays}
	}
	return tea.Batch(cmd, a.waitForPlay())
}

func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return a, tea.Quit
		}
	case tea.WindowSizeMsg:
		a.list.SetWidth(msg.Width)
		a.list.SetHeight(msg.Height - 2) // leave some space for status bar
		return a, nil
	case playsMsg:
		log.Infof("Received %d plays", len(msg.plays))
		cmds := make([]tea.Cmd, len(msg.plays))
		for _, play := range msg.plays {
			cmds = append(cmds, a.list.InsertItem(len(a.list.Items()), playItem{play: play}))
		}
		return a, tea.Sequence(cmds...)
	case playMsg:
		log.Infof("Received play: %s by %s", msg.play.TrackName, msg.play.UserDid)
		return a, tea.Batch(
			a.list.InsertItem(0, playItem{play: msg.play}),
			a.waitForPlay(), // wait for the next play
		)
	}
	l, cmd := a.list.Update(msg)
	a.list = l
	return a, cmd
}

func (a *App) View() string {
	return a.list.View()
}
