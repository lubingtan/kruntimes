package bash

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
)

const (
	defaultSessionFilePageSize = 100
	maxSessionFilePageSize     = 1000
	sessionFilePageTokenV1     = 1
)

type sessionFilePage struct {
	path  string
	after string
	limit int
}

type sessionFilePageToken struct {
	Version int    `json:"v"`
	Path    string `json:"path"`
	After   string `json:"after"`
}

func sessionFilePageRequest(request *pb.ListSessionFilesRequest) (sessionFilePage, error) {
	page := sessionFilePage{path: request.GetPath(), limit: defaultSessionFilePageSize}
	if request.GetLimit() != 0 {
		if request.GetLimit() < 0 || request.GetLimit() > maxSessionFilePageSize {
			return sessionFilePage{}, fmt.Errorf("limit must be between 1 and %d", maxSessionFilePageSize)
		}
		page.limit = int(request.GetLimit())
	}
	if request.GetPageToken() == "" {
		return page, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(request.GetPageToken())
	if err != nil {
		return sessionFilePage{}, fmt.Errorf("page token is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var token sessionFilePageToken
	if err := decoder.Decode(&token); err != nil {
		return sessionFilePage{}, fmt.Errorf("page token is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return sessionFilePage{}, fmt.Errorf("page token is invalid")
	}
	if token.Version != sessionFilePageTokenV1 || token.Path != page.path || token.After == "" {
		return sessionFilePage{}, fmt.Errorf("page token does not match the requested directory")
	}
	page.after = token.After
	return page, nil
}

func (p sessionFilePage) nextToken(after string) (string, error) {
	encoded, err := json.Marshal(sessionFilePageToken{Version: sessionFilePageTokenV1, Path: p.path, After: after})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// readSessionFilePage retains only pageSize+1 lexicographically earliest
// matching children, so a large directory cannot grow response or retained
// memory without bound.
func readSessionFilePage(path, after string, pageSize int) ([]os.DirEntry, bool, error) {
	directory, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer directory.Close()

	entries := make([]os.DirEntry, 0, pageSize+1)
	for {
		batch, err := directory.ReadDir(128)
		for _, entry := range batch {
			if entry.Name() <= after {
				continue
			}
			index, _ := slices.BinarySearchFunc(entries, entry, func(left, right os.DirEntry) int {
				return compareStrings(left.Name(), right.Name())
			})
			entries = append(entries, nil)
			copy(entries[index+1:], entries[index:])
			entries[index] = entry
			if len(entries) > pageSize+1 {
				entries = entries[:pageSize+1]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, err
		}
	}
	if len(entries) <= pageSize {
		return entries, false, nil
	}
	return entries[:pageSize], true, nil
}

func compareStrings(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
