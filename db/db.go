package db

import (
	log "github.com/sirupsen/logrus"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// {
//   "$type": "fm.teal.alpha.feed.play",
//   "artists": [
//     {
//       "artistMbId": "9eef0a84-41bb-4b34-80f5-4f37cbc54ad8",
//       "artistName": "Chez Damier"
//     },
//     {
//       "artistMbId": "74de5c4e-e839-43f3-b1bf-1718a855c75d",
//       "artistName": "Stacey Pullen"
//     }
//   ],
//   "duration": 206,
//   "originUrl": "https://www.last.fm/music/Chez+Damier+\u0026+Stacey+Pullen/_/Forever+Monna",
//   "trackName": "Forever Monna",
//   "playedTime": "2026-02-16T23:00:02Z",
//   "releaseMbId": "9f36bebd-a65e-4b79-9c81-d53eb0ed86e4",
//   "releaseName": "Timeless",
//   "recordingMbId": "896d12e2-7edf-459e-b091-f79747f2381d",
//   "submissionClientAgent": "piper/v0.0.5",
//   "musicServiceBaseDomain": "last.fm"
// }

type User struct {
	Did    string `gorm:"primaryKey"`
	Handle string
	Rev    string
}

type Artist struct {
	ID   uint   `gorm:"primaryKey"`
	MBID string `json:"artistMbId,omitempty"`
	Name string `json:"artistName"`
}

type Play struct {
	URI     string `gorm:"primaryKey"`
	UserDid string

	User User `gorm:"foreignKey:UserDid"`

	Artists                []Artist `gorm:"many2many:play_artists;"`
	Duration               int
	OriginUrl              string
	TrackName              string
	PlayedTime             time.Time
	ReleaseMBID            *string
	ReleaseName            string
	RecordingMBID          *string
	SubmissionClientAgent  string
	MusicServiceBaseDomain string
}

func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&User{}, &Play{}, &Artist{})
}

func NewDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open("teal.db"))
	if err != nil {
		return nil, err
	}
	db.Logger = logger.New(
		log.WithField("component", "gorm"),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)
	err = db.AutoMigrate(&User{}, &Play{}, &Artist{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

type Store struct {
	DB *gorm.DB

	artistCache map[string]Artist
}

func NewStore(db *gorm.DB) *Store {
	return &Store{DB: db, artistCache: make(map[string]Artist)}
}

func (s *Store) Begin() *gorm.DB {
	return s.DB.Begin()
}

func (s *Store) ListRecentPlays(limit int) ([]*Play, error) {
	var plays []*Play
	err := s.DB.Preload("Artists").Preload("User").Order("played_time desc").Limit(limit).Find(&plays).Error
	return plays, err
}

func (s *Store) SavePlaysBatch(db *gorm.DB, plays []*Play) error {
	// cache artists to prevent querying for name/mbid combo
	artistCache := make(map[string]Artist)

	for i := range plays {
		play := plays[i]
		for j := range play.Artists {
			artist := &play.Artists[j]
			cacheKey := artist.Name + "|" + artist.MBID
			cached, ok := artistCache[cacheKey]
			if ok {
				play.Artists[j] = cached
				continue
			}
			var existing Artist
			err := db.Where("name = ? AND mb_id IS ?", artist.Name, artist.MBID).
				FirstOrCreate(&existing, artist).Error
			if err != nil {
				return err
			}
			play.Artists[j] = existing
			artistCache[cacheKey] = existing
		}
	}

	return db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).CreateInBatches(plays, 100).Error
}

func (s *Store) SavePlay(db *gorm.DB, play *Play) error {
	for i := range play.Artists {
		artist := &play.Artists[i]
		var existing Artist
		err := db.Where("name = ? AND mb_id IS ?", artist.Name, artist.MBID).
			FirstOrCreate(&existing, artist).Error
		if err != nil {
			return err
		}
		play.Artists[i] = existing
	}

	// save play with associated artists
	return db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(play).Error

}

func (s *Store) GetUserRev(did string) (string, error) {
	var user User
	err := s.DB.First(&user, "did = ?", did).Error
	if err != nil {
		return "", err
	}
	return user.Rev, nil
}

func (s *Store) UpdateUser(db *gorm.DB, user *User) error {
	return db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Model(&User{}).Create(user).Error
}
