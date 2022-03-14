// Copyright 2019 The nutsdb Author. All rights reserved.
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

package set

import (
	"errors"
)

var (
	// ErrKeyNotExist is returned when the key not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyNotExist is returned when the key not exist.
	ErrKeyNotExist = errors.New("key not exist")
)

// Set represents the Set.
type Set struct {
	M map[string]map[string]struct{}
}

// New returns a newly initialized Set Object that implements the Set.
func New() *Set {
	return &Set{
		M: make(map[string]map[string]struct{}),
	}
}

// SAdd adds the specified members to the set stored at key.
func (s *Set) SAdd(key string, items ...[]byte) error {
	if _, ok := s.M[key]; !ok {
		s.M[key] = make(map[string]struct{})
	}

	for _, item := range items {
		s.M[key][string(item)] = struct{}{}
	}

	return nil
}

//SRem removes the specified members from the set stored at key.
func (s *Set) SRem(key string, items ...[]byte) error {
	if _, ok := s.M[key]; !ok {
		return ErrKeyNotFound
	}

	if len(items[0]) == 0 {
		return errors.New("item empty")
	}

	for _, item := range items {
		delete(s.M[key], string(item))
	}

	return nil
}

// SHasKey returns if has the set at given key.
func (s *Set) SHasKey(key string) bool {
	if _, ok := s.M[key]; !ok {
		return false
	}

	return true
}

// SPop removes and returns one or more random elements from the set value store at key.
func (s *Set) SPop(key string) []byte {
	if !s.SHasKey(key) {
		return nil
	}

	for item := range s.M[key] {
		delete(s.M[key], item)
		return []byte(item)
	}

	return nil
}

//SCard Returns the set cardinality (number of elements) of the set stored at key.
func (s *Set) SCard(key string) int {
	if !s.SHasKey(key) {
		return 0
	}

	return len(s.M[key])
}

//SDiff Returns the members of the set resulting from the difference between the first set and all the successive sets.
func (s *Set) SDiff(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		if _, ok := s.M[key2][item1]; !ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

//SInter Returns the members of the set resulting from the intersection of all the given sets.
func (s *Set) SInter(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		if _, ok := s.M[key2][item1]; ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

// checkKey1AndKey2 returns if key1 and key2 exists.
func (s *Set) checkKey1AndKey2(key1, key2 string) (list [][]byte, err error) {
	if _, ok := s.M[key1]; !ok {
		return nil, errors.New("set1 is not exists")
	}

	if _, ok := s.M[key2]; !ok {
		return nil, errors.New("set2 is not exists")
	}

	return nil, nil
}

//SIsMember Returns if member is a member of the set stored at key.
func (s *Set) SIsMember(key string, item []byte) bool {
	if _, ok := s.M[key]; !ok {
		return false
	}

	if _, ok := s.M[key][string(item)]; ok {
		return true
	}

	return false
}

// SAreMembers Returns if members are members of the set stored at key.
// For multiple items it returns true only if all of  the items exist.
func (s *Set) SAreMembers(key string, items ...[]byte) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, ErrKeyNotExist
	}

	for _, item := range items {
		if _, ok := s.M[key][string(item)]; !ok {
			return false, errors.New("item not exits")
		}
	}

	return true, nil
}

// SMembers returns all the members of the set value stored at key.
func (s *Set) SMembers(key string) (list [][]byte, err error) {
	if _, ok := s.M[key]; !ok {
		return nil, errors.New("set not exists")
	}

	for item := range s.M[key] {
		list = append(list, []byte(item))
	}

	return
}

// SMove moves member from the set at source to the set at destination.
func (s *Set) SMove(key1, key2 string, item []byte) (bool, error) {
	if !s.SHasKey(key1) {
		return false, errors.New("key1 is not exists")
	}

	if !s.SHasKey(key2) {
		return false, errors.New("key2 is not exists")
	}

	if _, ok := s.M[key2][string(item)]; !ok {
		s.SAdd(key2, item)
	}

	s.SRem(key1, item)

	return true, nil
}

//SUnion returns the members of the set resulting from the union of all the given sets.
func (s *Set) SUnion(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		list = append(list, []byte(item1))
	}

	for item2 := range s.M[key2] {
		if _, ok := s.M[key1][item2]; !ok {
			list = append(list, []byte(item2))
		}
	}

	return
}
