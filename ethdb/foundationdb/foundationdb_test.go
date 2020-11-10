// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package foundationdb

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestLevelDB(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			fdb.MustAPIVersion(620)
			db, err := fdb.OpenDefault()
			if err != nil {
				t.Fatal(err)
			}

			_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.ClearRange(fdb.KeyRange{Begin: fdb.Key(""), End: fdb.Key{0xFF}})
				return nil, nil
			})
			if err != nil {
				panic(err)
			}

			return &Database{
				prefix: []byte{1},
				db:     &db,
			}
		})
	})
}
