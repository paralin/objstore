package cli

import (
	"os"

	"github.com/aperturerobotics/objstore/db"
	"github.com/dgraph-io/badger"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	dbadger "github.com/aperturerobotics/objstore/db/badger"
	"github.com/aperturerobotics/objstore/db/inmem"
)

// DbFlags are the flags we append for setting shell connection arguments.
var DbFlags []cli.Flag

var cliDbArgs = struct {
	// DbType is the DB type to use.
	DbType string
	// DbPath is the path to store data in.
	DbPath string
}{
	DbType: "badger",
	DbPath: "./data",
}

// Ctor builds a database implementation.
type Ctor func(path string) (db.Db, error)

var cliDbImpls = map[string]Ctor{
	"inmem": func(path string) (db.Db, error) {
		return inmem.NewInmemDb(), nil
	},
	"badger": func(path string) (db.Db, error) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}

		badgerOpts := badger.DefaultOptions
		badgerOpts.Dir = path
		badgerOpts.ValueDir = path
		bdb, err := badger.Open(badgerOpts)
		if err != nil {
			return nil, err
		}

		return dbadger.NewBadgerDB(bdb), nil
	},
}

// RegisterCtor registers a command-line database constructor.
func RegisterCtor(id string, ctor Ctor) {
	cliDbImpls[id] = ctor
}

func init() {
	DbFlags = append(
		DbFlags,
		cli.StringFlag{
			Name:        "db-type",
			Usage:       "The DB type to use, badgerdb is the only supported value.",
			EnvVar:      "DB_TYPE",
			Value:       cliDbArgs.DbType,
			Destination: &cliDbArgs.DbType,
		},
		cli.StringFlag{
			Name:        "db-path",
			Usage:       "The path to store data in.",
			EnvVar:      "DB_PATH",
			Value:       cliDbArgs.DbPath,
			Destination: &cliDbArgs.DbPath,
		},
	)
}

// BuildCliDb builds the db from CLI args.
func BuildCliDb(log *logrus.Entry) (db.Db, error) {
	dbType := cliDbArgs.DbType
	ctor, ok := cliDbImpls[dbType]
	if !ok {
		return nil, errors.Errorf("unsupported db type: %s", dbType)
	}

	return ctor(cliDbArgs.DbPath)
}
