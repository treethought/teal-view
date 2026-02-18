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

type Feed struct {
	store    *db.Store
	consumer *at.Consumer
	events   <-chan *db.Play
	list     list.Model
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
	localtime := p.play.PlayedTime.Local()
	return fmt.Sprintf("@%s at %s",p.play.User.Handle, localtime.Format("2006-01-02 15:04:05"))
}
func (p playItem) FilterValue() string { return p.Title() + " " + p.play.User.Handle }

func NewFeed(store *db.Store, consumer *at.Consumer) *Feed {
	del := list.DefaultDelegate{
		ShowDescription: true,
		Styles:          list.NewDefaultItemStyles(),
	}
	del.SetHeight(2)

	l := list.New(nil, del, 80, 20)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(true)
	return &Feed{
		store:    store,
		consumer: consumer,
		list:     l,
	}
}
func (m *Feed) listenEvents() tea.Cmd {
	return func() tea.Msg {
		play := <-m.events
		return playMsg{play: play}
	}
}

func (m *Feed) Init() tea.Cmd {
	m.events = m.consumer.SubPlays("*") // subscribe to plays for a specific DID
	cmd := func() tea.Msg {
		plays, err := m.store.ListRecentPlays(100)
		if err != nil {
			log.Errorf("Failed to load recent plays: %v", err)
			return nil
		}
		return playsMsg{plays: plays}
	}
	return tea.Batch(cmd, m.listenEvents())
}

func (m *Feed) SetSize(width, height int) {
	m.list.SetWidth(width)
	m.list.SetHeight(height - 2) // leave some space for status bar
}

func (m *Feed) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
	case playsMsg:
		log.Infof("Received %d plays", len(msg.plays))
		cmds := make([]tea.Cmd, len(msg.plays))
		for _, play := range msg.plays {
			cmds = append(cmds, m.list.InsertItem(len(m.list.Items()), playItem{play: play}))
		}
		return m, tea.Sequence(cmds...)
	case playMsg:
		log.Infof("Received play: %s by %s", msg.play.TrackName, msg.play.UserDid)
		return m, tea.Batch(
			m.list.InsertItem(0, playItem{play: msg.play}),
			m.listenEvents(), // wait for the next play
		)
	}
	l, cmd := m.list.Update(msg)
	m.list = l
	return m, cmd
}

func (m *Feed) View() string {
	return m.list.View()
}
