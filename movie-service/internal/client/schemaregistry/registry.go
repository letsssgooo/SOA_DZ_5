package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	protobufType     = "PROTOBUF"
	requestTimeout   = 5 * time.Second
	maxResponseBytes = 1 << 20
)

func LookupProtobufID(ctx context.Context, baseURL, subject, schema string) (uint32, error) {
	requestBody, err := json.Marshal(struct {
		SchemaType string `json:"schemaType"`
		Schema     string `json:"schema"`
	}{SchemaType: protobufType, Schema: schema})
	if err != nil {
		return 0, fmt.Errorf("encode schema lookup: %w", err)
	}

	endpoint := strings.TrimRight(baseURL, "/") + "/subjects/" + url.PathEscape(subject) + "?normalize=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return 0, fmt.Errorf("create schema lookup request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	client := &http.Client{Timeout: requestTimeout}
	res, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("look up schema %s: %w", subject, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("look up schema %s: HTTP %s", subject, res.Status)
	}

	var registered struct {
		ID uint32 `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, maxResponseBytes)).Decode(&registered); err != nil {
		return 0, fmt.Errorf("decode schema lookup for %s: %w", subject, err)
	}
	if registered.ID == 0 {
		return 0, fmt.Errorf("invalid registry response for %s", subject)
	}
	return registered.ID, nil
}
