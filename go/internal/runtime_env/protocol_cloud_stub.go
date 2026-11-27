// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !cloud

package runtime_env

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var sharedCloudTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	TLSHandshakeTimeout:   10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
	IdleConnTimeout:       90 * time.Second,
}

// cloudHandlers returns pure-Go HTTP handlers for GS, S3, Azure Blob Storage,
// and ABFSS so cloud URIs work in the default Bazel build without pulling the
// heavy cloud SDK dependency trees.
func cloudHandlers(opts *DownloadOptions) []protocolHandler {
	timeout := 300 * time.Second
	if opts != nil {
		// Preserve opts.HTTPTimeout == 0 as "no timeout" (matching
		// httpProtocolHandler), so large archives are not aborted mid-download.
		timeout = opts.HTTPTimeout
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: sharedCloudTransport,
	}
	return []protocolHandler{
		&gsProtocolHandler{client: client},
		&s3ProtocolHandler{client: client},
		&azureProtocolHandler{client: client},
		&abfssProtocolHandler{client: client},
	}
}

// cloudProtocols returns all cloud storage protocols supported by the pure-Go
// handlers in the default build.
func cloudProtocols() []Protocol {
	return []Protocol{
		ProtocolS3,
		ProtocolGS,
		ProtocolAzure,
		ProtocolABFSS,
	}
}

func copyResponseToWriter(resp *http.Response, destWriter io.Writer, opts *DownloadOptions) error {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	bufSize := 32 * 1024
	if opts != nil && opts.BufferSize > 0 {
		bufSize = opts.BufferSize
	}
	bw := bufio.NewWriterSize(destWriter, bufSize)
	if _, err := io.Copy(bw, resp.Body); err != nil {
		return err
	}
	return bw.Flush()
}

// gsProtocolHandler downloads gs://bucket/object URIs via the GCS JSON API.
type gsProtocolHandler struct {
	client *http.Client
}

func (h *gsProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolGS
}

func (h *gsProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "GS")
	if err != nil {
		return err
	}
	if parts.Bucket == "" || parts.ObjectName == "" {
		return fmt.Errorf("invalid GS URI format: %s", sourceURI)
	}

	base := strings.TrimRight(os.Getenv("STORAGE_EMULATOR_HOST"), "/")
	if base == "" {
		base = "https://storage.googleapis.com"
	} else if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	endpoint := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media",
		base, url.PathEscape(parts.Bucket), url.PathEscape(parts.ObjectName))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to build GS request: %w", err)
	}
	if token := resolveGCSToken(ctx, h.client); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download GS object %q from bucket %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	if err := copyResponseToWriter(resp, destWriter, opts); err != nil {
		return fmt.Errorf("failed to download GS object %q from bucket %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	return nil
}

var (
	gceTokenMu     sync.Mutex
	gceCachedToken string
	gceTokenExpiry time.Time
)

func resolveGCSToken(ctx context.Context, client *http.Client) string {
	if tok := strings.TrimSpace(os.Getenv("GOOGLE_OAUTH_ACCESS_TOKEN")); tok != "" {
		return tok
	}
	gceTokenMu.Lock()
	if gceCachedToken != "" && time.Now().Before(gceTokenExpiry) {
		tok := gceCachedToken
		gceTokenMu.Unlock()
		return tok
	}
	gceTokenMu.Unlock()

	host := strings.TrimSpace(os.Getenv("GCE_METADATA_HOST"))
	if host == "" {
		host = "metadata.google.internal"
	}
	metaCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	metaURL := "http://" + strings.TrimRight(host, "/") +
		"/computeMetadata/v1/instance/service-accounts/default/token"
	req, err := http.NewRequestWithContext(metaCtx, http.MethodGet, metaURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var tokResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokResp); err != nil {
		return ""
	}
	if tokResp.AccessToken != "" && tokResp.ExpiresIn > 60 {
		gceTokenMu.Lock()
		gceCachedToken = tokResp.AccessToken
		gceTokenExpiry = time.Now().Add(time.Duration(tokResp.ExpiresIn-60) * time.Second)
		gceTokenMu.Unlock()
	}
	return tokResp.AccessToken
}

// s3ProtocolHandler downloads s3://bucket/key URIs with optional AWS SigV4 signing.
type s3ProtocolHandler struct {
	client *http.Client
}

func (h *s3ProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolS3
}

func (h *s3ProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "S3")
	if err != nil {
		return err
	}
	if parts.Bucket == "" || parts.ObjectName == "" {
		return fmt.Errorf("invalid S3 URI format: %s", sourceURI)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}

	customEndpoint := strings.TrimRight(os.Getenv("AWS_ENDPOINT_URL_S3"), "/")
	if customEndpoint == "" {
		customEndpoint = strings.TrimRight(os.Getenv("AWS_ENDPOINT_URL"), "/")
	}
	var targetURL string
	if customEndpoint != "" {
		targetURL = fmt.Sprintf("%s/%s/%s", customEndpoint, url.PathEscape(parts.Bucket), escapeS3Key(parts.ObjectName))
	} else {
		targetURL = fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", parts.Bucket, region, escapeS3Key(parts.ObjectName))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build S3 request: %w", err)
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")
	if accessKey != "" && secretKey != "" {
		signS3RequestV4(req, accessKey, secretKey, sessionToken, region, time.Now().UTC())
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download S3 object %q from bucket %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	if err := copyResponseToWriter(resp, destWriter, opts); err != nil {
		return fmt.Errorf("failed to download S3 object %q from bucket %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	return nil
}

func escapeS3Key(key string) string {
	segs := strings.Split(strings.TrimPrefix(key, "/"), "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

func signS3RequestV4(req *http.Request, accessKey, secretKey, sessionToken, region string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	emptyHash := sha256Hex(nil)

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", emptyHash)
	if sessionToken != "" {
		req.Header.Set("x-amz-security-token", sessionToken)
	}

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := req.URL.Query().Encode()

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, emptyHash, amzDate)
	if sessionToken != "" {
		signedHeaders += ";x-amz-security-token"
		canonicalHeaders += fmt.Sprintf("x-amz-security-token:%s\n", sessionToken)
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		emptyHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

// azureProtocolHandler downloads azure://container/blob URIs via Azure Blob Storage REST API.
type azureProtocolHandler struct {
	client *http.Client
}

func (h *azureProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolAzure
}

func (h *azureProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "Azure")
	if err != nil {
		return err
	}
	if parts.Bucket == "" || parts.ObjectName == "" {
		return fmt.Errorf("invalid Azure URI format: %s", sourceURI)
	}

	account := strings.TrimSpace(os.Getenv("AZURE_STORAGE_ACCOUNT"))
	endpoint := strings.TrimRight(os.Getenv("AZURE_STORAGE_BLOB_ENDPOINT"), "/")
	if endpoint == "" {
		if account == "" {
			return fmt.Errorf("AZURE_STORAGE_ACCOUNT environment variable is required for Azure protocol")
		}
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", account)
	}

	sas := strings.TrimPrefix(strings.TrimSpace(os.Getenv("AZURE_STORAGE_SAS_TOKEN")), "?")
	targetURL := fmt.Sprintf("%s/%s/%s", endpoint, url.PathEscape(parts.Bucket), escapeS3Key(parts.ObjectName))
	if sas != "" {
		targetURL += "?" + sas
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build Azure request: %w", err)
	}
	if sas == "" {
		if err := applyAzureAuth(req, account, parts.Bucket, parts.ObjectName); err != nil {
			return err
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download Azure blob %q from container %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	if err := copyResponseToWriter(resp, destWriter, opts); err != nil {
		return fmt.Errorf("failed to download Azure blob %q from container %q: %w", parts.ObjectName, parts.Bucket, err)
	}
	return nil
}

// abfssProtocolHandler downloads abfss://filesystem@account.dfs.core.windows.net/path URIs.
type abfssProtocolHandler struct {
	client *http.Client
}

func (h *abfssProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolABFSS
}

func (h *abfssProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	u, err := url.Parse(sourceURI)
	if err != nil {
		return fmt.Errorf("invalid ABFSS URI format: %s: %w", sourceURI, err)
	}
	var filesystem, account, host string
	if u.User != nil && u.User.Username() != "" {
		filesystem = u.User.Username()
		host = u.Host
		account = strings.SplitN(host, ".", 2)[0]
	} else {
		filesystem = u.Host
		account = strings.TrimSpace(os.Getenv("AZURE_STORAGE_ACCOUNT"))
		if account != "" {
			host = fmt.Sprintf("%s.dfs.core.windows.net", account)
		}
	}
	objectPath := strings.TrimPrefix(u.Path, "/")
	if filesystem == "" || objectPath == "" {
		return fmt.Errorf("invalid ABFSS URI format: %s", sourceURI)
	}

	endpoint := strings.TrimRight(os.Getenv("AZURE_STORAGE_DFS_ENDPOINT"), "/")
	if endpoint == "" {
		if host == "" {
			return fmt.Errorf("AZURE_STORAGE_ACCOUNT environment variable or account host in URI is required for ABFSS protocol")
		}
		endpoint = "https://" + host
	}

	sas := strings.TrimPrefix(strings.TrimSpace(os.Getenv("AZURE_STORAGE_SAS_TOKEN")), "?")
	targetURL := fmt.Sprintf("%s/%s/%s", endpoint, url.PathEscape(filesystem), escapeS3Key(objectPath))
	if sas != "" {
		targetURL += "?" + sas
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build ABFSS request: %w", err)
	}
	if sas == "" {
		if err := applyAzureAuth(req, account, filesystem, objectPath); err != nil {
			return err
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download ABFSS file %q from filesystem %q: %w", objectPath, filesystem, err)
	}
	if err := copyResponseToWriter(resp, destWriter, opts); err != nil {
		return fmt.Errorf("failed to download ABFSS file %q from filesystem %q: %w", objectPath, filesystem, err)
	}
	return nil
}

func applyAzureAuth(req *http.Request, account, container, objectPath string) error {
	if tok := strings.TrimSpace(os.Getenv("AZURE_STORAGE_BEARER_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("x-ms-version", "2020-10-02")
		return nil
	}
	keyB64 := strings.TrimSpace(os.Getenv("AZURE_STORAGE_KEY"))
	if keyB64 == "" || account == "" {
		return nil
	}
	keyBytes, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil {
		return fmt.Errorf("invalid AZURE_STORAGE_KEY: %w", err)
	}
	dateStr := time.Now().UTC().Format(http.TimeFormat)
	version := "2020-10-02"
	req.Header.Set("x-ms-date", dateStr)
	req.Header.Set("x-ms-version", version)

	canonicalHeaders := fmt.Sprintf("x-ms-date:%s\nx-ms-version:%s\n", dateStr, version)
	canonicalResource := fmt.Sprintf("/%s/%s/%s", account, container, strings.TrimPrefix(objectPath, "/"))
	stringToSign := fmt.Sprintf("%s\n\n\n\n\n\n\n\n\n\n\n\n%s%s", req.Method, canonicalHeaders, canonicalResource)

	mac := hmac.New(sha256.New, keyBytes)
	mac.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	req.Header.Set("Authorization", fmt.Sprintf("SharedKey %s:%s", account, sig))
	return nil
}
