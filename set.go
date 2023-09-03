// Copyright 2023 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"errors"
	"hash/fnv"
)

var (
	// ErrSetNotExist is returned when the key does not exist.
	ErrSetNotExist = errors.New("set not exist")

	// ErrSetMemberNotExist is returned when the member of set does not exist
	ErrSetMemberNotExist = errors.New("set member not exist")

	// ErrMemberEmpty is returned when the item received is nil
	ErrMemberEmpty = errors.New("item empty")
)

var fnvHash = fnv.New32a()

type Set struct {
	M map[string]map[uint32]*Record
}

func NewSet() *Set {
	return &Set{
		M: map[string]map[uint32]*Record{},
	}
}

// SAdd adds the specified members to the set stored at key.
func (s *Set) SAdd(key string, values [][]byte, records []*Record) error {
	set, ok := s.M[key]
	if !ok {
		s.M[key] = map[uint32]*Record{}
		set = s.M[key]
	}

	for i, value := range values {
		hash, err := getFnv32(value)
		if err != nil {
			return err
		}
		set[hash] = records[i]
	}

	return nil
}

// SRem removes the specified members from the set stored at key.
func (s *Set) SRem(key string, values ...[]byte) error {
	set, ok := s.M[key]
	if !ok {
		return ErrSetNotExist
	}

	if len(values) == 0 || values[0] == nil {
		return ErrMemberEmpty
	}

	for _, value := range values {
		hash, err := getFnv32(value)
		if err != nil {
			return err
		}
		delete(set, hash)
	}

	return nil
}

// SHasKey returns whether it has the set at given key.
func (s *Set) SHasKey(key string) bool {
	if _, ok := s.M[key]; ok {
		return true
	}
	return false
}

// SPop removes and returns one or more random elements from the set value store at key.
func (s *Set) SPop(key string) *Record {
	if !s.SHasKey(key) {
		return nil
	}

	for hash, record := range s.M[key] {
		delete(s.M[key], hash)
		return record
	}

	return nil
}

// SCard Returns the set cardinality (number of elements) of the set stored at key.
func (s *Set) SCard(key string) int {
	if !s.SHasKey(key) {
		return 0
	}

	return len(s.M[key])
}

// SDiff Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (s *Set) SDiff(key1, key2 string) ([]*Record, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records := make([]*Record, 0)

	for hash, record := range s.M[key1] {
		if _, ok := s.M[key2][hash]; !ok {
			records = append(records, record)
		}
	}
	return records, nil
}

// SInter Returns the members of the set resulting from the intersection of all the given sets.
func (s *Set) SInter(key1, key2 string) ([]*Record, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records := make([]*Record, 0)

	for hash, record := range s.M[key1] {
		if _, ok := s.M[key2][hash]; ok {
			records = append(records, record)
		}
	}
	return records, nil
}

// SIsMember Returns if member is a member of the set stored at key.
func (s *Set) SIsMember(key string, value []byte) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, ErrSetNotExist
	}

	hash, err := getFnv32(value)
	if err != nil {
		return false, err
	}

	if _, ok := s.M[key][hash]; ok {
		return true, nil
	}

	return false, nil
}

// SAreMembers Returns if members are members of the set stored at key.
// For multiple items it returns true only if all the items exist.
func (s *Set) SAreMembers(key string, values ...[]byte) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, ErrSetNotExist
	}

	for _, value := range values {

		hash, err := getFnv32(value)
		if err != nil {
			return false, err
		}

		if _, ok := s.M[key][hash]; !ok {
			return false, nil
		}
	}

	return true, nil
}

// SMembers returns all the members of the set value stored at key.
func (s *Set) SMembers(key string) ([]*Record, error) {
	if _, ok := s.M[key]; !ok {
		return nil, ErrSetNotExist
	}

	records := make([]*Record, 0)

	for _, record := range s.M[key] {
		records = append(records, record)
	}

	return records, nil
}

// SMove moves member from the set at source to the set at destination.
func (s *Set) SMove(key1, key2 string, value []byte) (bool, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return false, ErrSetNotExist
	}

	set1, set2 := s.M[key1], s.M[key2]

	hash, err := getFnv32(value)
	if err != nil {
		return false, err
	}

	var (
		member *Record
		ok     bool
	)

	if member, ok = set1[hash]; !ok {
		return false, ErrSetMemberNotExist
	}

	if _, ok = set2[hash]; !ok {
		err = s.SAdd(key2, [][]byte{value}, []*Record{member})
		if err != nil {
			return false, err
		}
	}

	err = s.SRem(key1, value)
	if err != nil {
		return false, err
	}

	return true, nil
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (s *Set) SUnion(key1, key2 string) ([]*Record, error) {
	if !s.SHasKey(key1) || !s.SHasKey(key2) {
		return nil, ErrSetNotExist
	}

	records, err := s.SMembers(key1)

	if err != nil {
		return nil, err
	}

	for hash, record := range s.M[key2] {
		if _, ok := s.M[key1][hash]; !ok {
			records = append(records, record)
		}
	}

	return records, nil
}
