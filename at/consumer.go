package at

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/treethought/teal-view/db"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	_ "github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"gorm.io/gorm"
)

var relayHost = "relay1.us-east.bsky.network"
var pdsService = "https://bsky.social"

var collection = "fm.teal.alpha.feed.play"

type Consumer struct {
	log         *log.Entry
	trackedDids map[string]struct{}
	rsc         *events.RepoStreamCallbacks
	store       *db.Store
	sched       events.Scheduler
	conn        *websocket.Conn
	evts        chan *comatproto.SyncSubscribeRepos_Commit
	client      *ATClient
	dir         identity.Directory
	revs        map[string]string
	playSubs    map[string]chan *db.Play
}

func NewConsumer(store *db.Store) *Consumer {
	baseDir := &identity.BaseDirectory{}
	cacheDir := identity.NewCacheDirectory(baseDir, 1000, 10*time.Minute, 10*time.Minute, 10*time.Minute)

	c := &Consumer{
		log:         log.WithField("module", "consumer"),
		trackedDids: make(map[string]struct{}),
		store:       store,
		client:      NewATClient(cacheDir),
		dir:         cacheDir,
		revs:        make(map[string]string),
		playSubs:    make(map[string]chan *db.Play),
	}
	c.rsc = &events.RepoStreamCallbacks{
		RepoCommit: c.handleCommitEvent,
	}
	c.sched = parallel.NewScheduler(10, 1000, "main-consumer", c.rsc.EventHandler)
	return c
}

func (c *Consumer) SubPlays(did string) <-chan *db.Play {
	ch := make(chan *db.Play, 100)
	c.playSubs[did] = ch
	return ch
}

func (c *Consumer) UnsubPlays(did string) {
	if sub, ok := c.playSubs[did]; ok {
		close(sub)
		delete(c.playSubs, did)
	}
}

func (c *Consumer) PublishPlay(play *db.Play) {
	if sub, ok := c.playSubs[play.UserDid]; ok {
		sub <- play
	}
	if sub, ok := c.playSubs["*"]; ok {
		sub <- play
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	u := fmt.Sprintf("wss://%s/xrpc/com.atproto.sync.subscribeRepos", relayHost)

	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	defer conn.Close()
	return events.HandleRepoStream(ctx, conn, c.sched, nil)
}

func (c *Consumer) TrackDid(did string) {
	c.trackedDids[did] = struct{}{}
}

func (c *Consumer) handleCommitEvent(evt *comatproto.SyncSubscribeRepos_Commit) error {
	if err := c.processEvent(context.Background(), evt); err != nil {
		c.log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("failed to process commit event")
	}
	return nil
}

func (c *Consumer) processEvent(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {

	var ops []*comatproto.SyncSubscribeRepos_RepoOp
	for _, op := range evt.Ops {
		if !strings.HasPrefix(op.Path, collection) {
			continue
		}
		switch op.Action {
		case "update", "create":
		default:
			continue
		}
		ops = append(ops, op)
	}

	if len(ops) == 0 {
		return nil
	}

	commit, _, err := repo.LoadRepoFromCAR(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		c.log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("failed to load repo from CAR in commit event")
		return err
	}

	lastRev, err := c.store.GetUserRev(evt.Repo)
	if err != nil && err != gorm.ErrRecordNotFound {
		c.log.WithError(err).WithField("did", evt.Repo).Error("failed to get user rev from database")
		return err
	}

	if lastRev != commit.Rev {
		if err := c.backfillUser(ctx, evt.Repo, lastRev); err != nil {
			c.log.WithError(err).WithFields(log.Fields{
				"did":      evt.Repo,
				"sinceRev": lastRev,
			}).Error("failed to backfill user from lastRev")
			return err
		}
	}
	c.log.WithFields(log.Fields{
		"did": evt.Repo,
		"rev": commit.Rev,
	}).Info("processing commit event")

	rr, err := repo.VerifyCommitMessage(ctx, evt)
	if err != nil {
		c.log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("failed to verify commit message")
		return err
	}

	plays := make([]*db.Play, 0, len(ops))
	for _, op := range ops {
		lf := log.Fields{
			"did":    evt.Repo,
			"rev":    evt.Rev,
			"path":   op.Path,
			"action": op.Action,
		}

		if op.Cid == nil {
			c.log.WithFields(lf).Warn("Op has no CID, skipping")
			continue
		}

		col, rkey, err := syntax.ParseRepoPath(op.Path)
		if err != nil {
			c.log.WithError(err).WithFields(lf).Error("failed to parse repo path in commit event")
			return err
		}
		if col.String() != collection {
			continue
		}

		recBytes, _, err := rr.GetRecordBytes(ctx, col, rkey)
		if err != nil {
			c.log.WithError(err).WithFields(lf).WithField("col", col.String()).WithField("rkey", rkey.String()).Error("failed to get record bytes from repo for commit event")
			return err
		}

		var play db.Play
		if err := unmarshalJson(recBytes, &play); err != nil {
			c.log.WithError(err).WithFields(lf).Error("failed to unmarshal commit event record bytes")
			return err
		}
		play.URI = fmt.Sprintf("at://%s/%s/%s", evt.Repo, col, rkey)
		play.UserDid = evt.Repo
		plays = append(plays, &play)

		c.log.WithFields(lf).Debug("processed play record")
	}

	tx := c.store.DB.Begin()
	if err := c.store.SavePlaysBatch(tx, plays); err != nil {
		c.log.WithError(err).WithFields(log.Fields{
			"did":   evt.Repo,
			"rev":   evt.Rev,
			"count": len(plays),
		}).Error("failed to save play records to db")
		tx.Rollback()
		return err
	}

	id, err := c.client.ResolveIdentity(evt.Repo)
	if err != nil {
		c.log.WithError(err).WithField("did", evt.Repo).Error("failed to resolve identity for user")
		return err
	}

	user := &db.User{
		Did:    evt.Repo,
		Handle: id.Handle.String(),
		Rev:    commit.Rev,
	}
	if err := c.store.UpdateUser(tx, user); err != nil {
		c.log.WithError(err).WithField("did", evt.Repo).Error("failed to update user in database")
		return err
	}
	if err := tx.Commit().Error; err != nil {
		c.log.WithError(err).WithField("did", evt.Repo).Error("failed to commit transaction")
		return err
	}
	for _, play := range plays {
		c.PublishPlay(play)
	}
	return nil
}

func unmarshalJson(cborBytes []byte, out any) error {
	m, err := atdata.UnmarshalCBOR(cborBytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal CBOR: %w", err)
	}
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal record to JSON: %w", err)
	}
	if err := json.Unmarshal(jsonBytes, out); err != nil {
		return fmt.Errorf("failed to unmarshal JSON into struct: %w", err)
	}
	return nil
}

func (c *Consumer) backfillUser(ctx context.Context, did string, lastRev string) error {
	lf := log.Fields{
		"did":     did,
		"lastRev": lastRev,
		"job":     "backfill",
	}
	c.log.WithFields(lf).Info("backfilling user")

	id, err := c.client.ResolveIdentity(did)
	if err != nil {
		return err
	}
	client := c.client.WithPDS(id.PDSEndpoint())

	resp, err := comatproto.SyncGetRepo(ctx, client, did, lastRev)
	if err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to get repo for user")
		return err
	}
	commit, r, err := repo.LoadRepoFromCAR(ctx, bytes.NewReader(resp))
	if err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to load repo from CAR for user")
		return err
	}

	plays := []*db.Play{}
	err = r.MST.Walk(func(key []byte, val cid.Cid) error {
		col, rkey, err := syntax.ParseRepoPath(string(key))
		if err != nil {
			c.log.WithError(err).WithFields(log.Fields{
				"key": string(key),
			}).Error("failed to parse repo path")
			return err
		}
		uri := fmt.Sprintf("at://%s/%s/%s", did, col, rkey)
		lf := log.Fields{
			"did":  did,
			"col":  col,
			"rkey": rkey,
			"cid":  val.String(),
			"uri":  uri,
			"job":  "backfill",
		}
		if col.String() != collection {
			return nil
		}
		recBytes, _, err := r.GetRecordBytes(ctx, col, rkey)
		if err != nil {
			c.log.WithError(err).WithFields(lf).Error("failed to get record bytes")
			return err
		}
		var play db.Play
		if err := unmarshalJson(recBytes, &play); err != nil {
			c.log.WithError(err).WithFields(lf).Error("failed to inmarshal record into model struct")
			return err
		}
		play.URI = fmt.Sprintf("at://%s/%s/%s", did, col, rkey)
		play.UserDid = did
		plays = append(plays, &play)
		return nil
	})
	if err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to walk repo MST")
		return err
	}

	tx := c.store.DB.Begin()
	if err := c.store.SavePlaysBatch(tx, plays); err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to save play record to database")
		tx.Rollback()
		return err
	}

	user := &db.User{
		Did:    did,
		Handle: id.Handle.String(),
		Rev:    commit.Rev,
	}
	if err := c.store.UpdateUser(tx, user); err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to update user in database during backfill")
		tx.Rollback()
		return err
	}
	if err := tx.Commit().Error; err != nil {
		c.log.WithError(err).WithFields(lf).Error("failed to commit transaction")
		return err
	}
	c.log.WithFields(log.Fields{
		"did":   did,
		"total": len(plays),
	}).Info("backfilled user")
	return nil
}
