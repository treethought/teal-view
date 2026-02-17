package main

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
	"github.com/bluesky-social/indigo/atproto/atclient"
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
var userAgent = "teal-view/0.1"

var collection = "fm.teal.alpha.feed.play"

type ATClient struct {
	*atclient.APIClient
	dir identity.Directory
}

func NewATClient(dir identity.Directory) *ATClient {
	baseDir := &identity.BaseDirectory{}
	cacheDir := identity.NewCacheDirectory(baseDir, 1000, 10*time.Minute, 10*time.Minute, 10*time.Minute)

	client := atclient.NewAPIClient(pdsService)
	client.Headers.Set("User-Agent", userAgent)

	return &ATClient{
		dir:       cacheDir,
		APIClient: client,
	}
}

func (c *ATClient) resolveIdentity(raw string) (*identity.Identity, error) {
	id, err := syntax.ParseAtIdentifier(raw)
	if err != nil {
		return nil, err
	}
	idd, err := c.dir.Lookup(context.Background(), id)
	if err != nil {
		log.WithError(err).WithField("id", id.String()).Error("Failed to resolve identity")
		return nil, err
	}
	return idd, nil
}

func (c *ATClient) WithPDS(host string) *ATClient {
	out := atclient.APIClient{
		Client:     c.Client,
		Host:       host,
		Auth:       c.Auth,
		Headers:    c.Headers.Clone(),
		AccountDID: c.AccountDID,
	}
	return &ATClient{
		dir:       c.dir,
		APIClient: &out,
	}
}

type Consumer struct {
	trackedDids map[string]struct{}
	rsc         *events.RepoStreamCallbacks
	store       *db.Store
	sched       events.Scheduler
	conn        *websocket.Conn
	evts        chan *comatproto.SyncSubscribeRepos_Commit
	client      *ATClient
	dir         identity.Directory
	revs        map[string]string
	pdsClients  map[string]*atclient.APIClient
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewConsumer(store *db.Store) *Consumer {
	baseDir := &identity.BaseDirectory{}
	cacheDir := identity.NewCacheDirectory(baseDir, 1000, 10*time.Minute, 10*time.Minute, 10*time.Minute)

	client := atclient.NewAPIClient(pdsService)
	client.Headers.Set("User-Agent", userAgent)

	c := &Consumer{
		trackedDids: make(map[string]struct{}),
		store:       store,
		client:      NewATClient(cacheDir),
		dir:         cacheDir,
		revs:        make(map[string]string),
	}
	c.rsc = &events.RepoStreamCallbacks{
		RepoCommit: c.handleCommitEvent,
	}
	c.sched = parallel.NewScheduler(10, 1000, "main-consumer", c.rsc.EventHandler)
	return c
}

func (c *Consumer) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
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
		log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("Failed to process commit event")
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

	commit, rr, err := repo.LoadRepoFromCAR(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("Failed to load repo from CAR in commit event")
		return err
	}

	lastRev, err := c.store.GetUserRev(evt.Repo)
	if err != nil && err != gorm.ErrRecordNotFound {
		log.WithError(err).WithField("did", evt.Repo).Error("Failed to get user rev from database")
		return err
	}

	if lastRev != commit.Rev {
		if err := c.backfillUser(ctx, evt.Repo, lastRev); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"did":      evt.Repo,
				"sinceRev": lastRev,
			}).Error("Failed to backfill user from lastRev")
			return err
		}
	}
	rr, err := repo.VerifyCommitMessage(ctx, evt)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"did": evt.Repo,
			"rev": evt.Rev,
		}).Error("Failed to verify commit message")
		return err
	}

	tx := c.store.DB.Begin()
	for _, op := range ops {
		lf := log.Fields{
			"did":    evt.Repo,
			"rev":    evt.Rev,
			"path":   op.Path,
			"action": op.Action,
		}

		if op.Cid == nil {
			log.WithFields(lf).Warn("Op has no CID, skipping")
			continue
		}

		col, rkey, err := syntax.ParseRepoPath(op.Path)
		if err != nil {
			log.WithError(err).WithFields(lf).Error("Failed to parse repo path in commit event")
			tx.Rollback()
			return err
		}
		if col.String() != collection {
			continue
		}
		recBytes, _, err := rr.GetRecordBytes(ctx, col, rkey)
		if err != nil {
			log.WithError(err).WithFields(lf).Error("Failed to get record bytes from repo")
			tx.Rollback()
			return err
		}
		var play db.Play
		if err := unmarshalJson(recBytes, &play); err != nil {
			log.WithError(err).WithFields(lf).Error("Failed to unmarshal commit event record bytes")
			tx.Rollback()
			return err
		}
		play.URI = fmt.Sprintf("at://%s/%s/%s", evt.Repo, col, rkey)
		play.UserDid = evt.Repo

		if err := c.store.SavePlay(tx, &play); err != nil {
			log.WithError(err).WithFields(lf).Error("Failed to save play record to database")
			tx.Rollback()
			return err
		}
		log.WithFields(lf).Debug("processed play record ")
	}

	id, err := c.client.resolveIdentity(evt.Repo)
	if err != nil {
		log.WithError(err).WithField("did", evt.Repo).Error("Failed to resolve identity for user")
		return err
	}

	user := &db.User{
		Did:    evt.Repo,
		Handle: id.Handle.String(),
		Rev:    commit.Rev,
	}
	if err := c.store.UpdateUser(tx, user); err != nil {
		log.WithError(err).WithField("did", evt.Repo).Error("Failed to update user in database")
		return err
	}
	return tx.Commit().Error
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
	log.WithFields(lf).Info("Backfilling user")

	id, err := c.client.resolveIdentity(did)
	if err != nil {
		return err
	}
	client := c.client.WithPDS(id.PDSEndpoint())

	resp, err := comatproto.SyncGetRepo(ctx, client, did, lastRev)
	if err != nil {
		log.WithError(err).WithFields(lf).Error("Failed to get repo for user")
		return err
	}
	commit, r, err := repo.LoadRepoFromCAR(ctx, bytes.NewReader(resp))
	if err != nil {
		log.WithError(err).WithFields(lf).Error("Failed to load repo from CAR for user")
		return err
	}
	log.WithFields(lf).Info("Loaded repo from CAR for user")

	tx := c.store.DB.Begin()

	total := 0
	err = r.MST.Walk(func(key []byte, val cid.Cid) error {
		col, rkey, err := syntax.ParseRepoPath(string(key))
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"key": string(key),
			}).Error("Failed to parse repo path")
			return err
		}
		lf := log.Fields{
			"did":  did,
			"col":  col,
			"rkey": rkey,
			"cid":  val.String(),
			"job":  "backfill",
		}
		if col.String() != collection {
			return nil
		}
		recBytes, _, err := r.GetRecordBytes(ctx, col, rkey)
		if err != nil {
			log.WithError(err).WithFields(lf).Error("failed to get record bytes")
			return err
		}
		rec, err := atdata.UnmarshalCBOR(recBytes)
		if err != nil {
			log.WithError(err).WithFields(lf).Error("failed to unmarshal CBOR record bytes")
			return err
		}
		// TODO implement cbor serialization on models
		d, err := json.Marshal(rec)
		if err != nil {
			log.WithError(err).WithFields(lf).Error("failed to marshal record")
			return err
		}
		var play db.Play
		if err := json.Unmarshal(d, &play); err != nil {
			log.WithError(err).WithFields(lf).Error("failed to inmarshal record into model struct")
			return err
		}
		play.URI = fmt.Sprintf("at://%s/%s/%s", did, col, rkey)
		play.UserDid = did

		if err := c.store.SavePlay(tx, &play); err != nil {
			log.WithError(err).WithFields(lf).Error("failed to save play record to database")
			return err
		}
		total++
		return nil
	})

	if err != nil {
		log.WithError(err).WithFields(lf).Error("failed to walk repo MST")
		tx.Rollback()
		return err
	}

	user := &db.User{
		Did:    did,
		Handle: id.Handle.String(),
		Rev:    commit.Rev,
	}
	if err := c.store.UpdateUser(tx, user); err != nil {
		log.WithError(err).WithFields(lf).Error("Failed to update user in database during backfill")
		tx.Rollback()
		return err
	}
	if err := tx.Commit().Error; err != nil {
		log.WithError(err).WithFields(lf).Error("failed to commit transaction")
		return err
	}
	log.WithFields(log.Fields{
		"did":   did,
		"total": total,
	}).Info("backfilled user")
	return nil
}
