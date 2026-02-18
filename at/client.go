package at

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/bluesky-social/indigo/atproto/atclient"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	_ "github.com/bluesky-social/indigo/events"
)

var userAgent = "teal-view/0.1"

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

func (c *ATClient) ResolveIdentity(raw string) (*identity.Identity, error) {
	id, err := syntax.ParseAtIdentifier(raw)
	if err != nil {
		return nil, err
	}
	idd, err := c.dir.Lookup(context.Background(), id)
	if err != nil {
		log.WithError(err).WithField("id", id.String()).Error("failed to resolve identity")
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
