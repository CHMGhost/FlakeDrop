package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"flakedrop/internal/common"
)

// HTTPPluginRegistry implements PluginRegistry using HTTP endpoints
type HTTPPluginRegistry struct {
	baseURL    string
	client     *http.Client
	cacheDir   string
	apiKey     string
	userAgent  string
	timeout    time.Duration
}

// LocalPluginRegistry implements PluginRegistry using local file system
type LocalPluginRegistry struct {
	registryDir string
	cacheDir    string
}

// PluginRegistryResponse represents the response from a plugin registry
type PluginRegistryResponse struct {
	Plugins    []PluginManifest `json:"plugins"`
	TotalCount int              `json:"total_count"`
	Page       int              `json:"page"`
	PageSize   int              `json:"page_size"`
}

// PluginSearchRequest represents a search request
type PluginSearchRequest struct {
	Query    string   `json:"query"`
	Category string   `json:"category,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	Author   string   `json:"author,omitempty"`
	Page     int      `json:"page,omitempty"`
	PageSize int      `json:"page_size,omitempty"`
}

// PluginDownloadInfo contains information about downloading a plugin
type PluginDownloadInfo struct {
	URL      string            `json:"url"`
	Hash     string            `json:"hash"`
	Size     int64             `json:"size"`
	Headers  map[string]string `json:"headers,omitempty"`
}

// NewHTTPPluginRegistry creates a new HTTP-based plugin registry
func NewHTTPPluginRegistry(baseURL, cacheDir string) *HTTPPluginRegistry {
	return &HTTPPluginRegistry{
		baseURL:   baseURL,
		cacheDir:  cacheDir,
		client:    &http.Client{Timeout: 30 * time.Second},
		userAgent: "snowflake-deploy-plugin-client/1.0",
		timeout:   30 * time.Second,
	}
}

// SetAPIKey sets the API key for authenticated requests
func (r *HTTPPluginRegistry) SetAPIKey(apiKey string) {
	r.apiKey = apiKey
}

// SetTimeout sets the request timeout
func (r *HTTPPluginRegistry) SetTimeout(timeout time.Duration) {
	r.timeout = timeout
	r.client.Timeout = timeout
}

// List returns available plugins from the registry
func (r *HTTPPluginRegistry) List(ctx context.Context) ([]PluginManifest, error) {
	endpoint := fmt.Sprintf("%s/plugins", r.baseURL)
	
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	r.setCommonHeaders(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch plugin list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}

	var response PluginRegistryResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Plugins, nil
}

// Get retrieves a specific plugin manifest
func (r *HTTPPluginRegistry) Get(ctx context.Context, name, version string) (*PluginManifest, error) {
	endpoint := fmt.Sprintf("%s/plugins/%s", r.baseURL, name)
	if version != "" {
		endpoint += fmt.Sprintf("/%s", version)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	r.setCommonHeaders(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch plugin: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("plugin %s not found", name)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}

	var manifest PluginManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	return &manifest, nil
}

// Search searches for plugins matching the query
func (r *HTTPPluginRegistry) Search(ctx context.Context, query string) ([]PluginManifest, error) {
	endpoint := fmt.Sprintf("%s/plugins/search", r.baseURL)

	searchReq := PluginSearchRequest{
		Query:    query,
		PageSize: 50,
	}

	reqBody, err := json.Marshal(searchReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	r.setCommonHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to search plugins: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}

	var response PluginRegistryResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Plugins, nil
}

// Download downloads a plugin from the registry
func (r *HTTPPluginRegistry) Download(ctx context.Context, name, version string) ([]byte, error) {
	// First get download info
	downloadInfo, err := r.getDownloadInfo(ctx, name, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get download info: %w", err)
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s-%s-%s", name, version, downloadInfo.Hash)
	if cachedData, err := r.getFromCache(cacheKey); err == nil {
		return cachedData, nil
	}

	// Download from registry
	req, err := http.NewRequestWithContext(ctx, "GET", downloadInfo.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create download request: %w", err)
	}

	r.setCommonHeaders(req)
	for key, value := range downloadInfo.Headers {
		req.Header.Set(key, value)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download plugin: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read download data: %w", err)
	}

	// Verify hash
	if downloadInfo.Hash != "" {
		hash := sha256.Sum256(data)
		expectedHash := fmt.Sprintf("%x", hash)
		if expectedHash != downloadInfo.Hash {
			return nil, fmt.Errorf("hash mismatch: expected %s, got %s", downloadInfo.Hash, expectedHash)
		}
	}

	// Cache the downloaded data
	if err := r.saveToCache(cacheKey, data); err != nil {
		// Log warning but don't fail the download
		fmt.Printf("Warning: failed to cache plugin data: %v\n", err)
	}

	return data, nil
}

// getDownloadInfo gets download information for a plugin
func (r *HTTPPluginRegistry) getDownloadInfo(ctx context.Context, name, version string) (*PluginDownloadInfo, error) {
	endpoint := fmt.Sprintf("%s/plugins/%s/download", r.baseURL, name)
	if version != "" {
		endpoint += fmt.Sprintf("?version=%s", url.QueryEscape(version))
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	r.setCommonHeaders(req)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get download info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}

	var downloadInfo PluginDownloadInfo
	if err := json.NewDecoder(resp.Body).Decode(&downloadInfo); err != nil {
		return nil, fmt.Errorf("failed to decode download info: %w", err)
	}

	return &downloadInfo, nil
}

// setCommonHeaders sets common HTTP headers
func (r *HTTPPluginRegistry) setCommonHeaders(req *http.Request) {
	req.Header.Set("User-Agent", r.userAgent)
	req.Header.Set("Accept", "application/json")
	
	if r.apiKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", r.apiKey))
	}
}

// getFromCache gets data from cache
func (r *HTTPPluginRegistry) getFromCache(key string) ([]byte, error) {
	if r.cacheDir == "" {
		return nil, fmt.Errorf("cache not configured")
	}

	cacheFile := filepath.Join(r.cacheDir, key)
	cacheFile, err := common.ValidatePath(cacheFile, r.cacheDir)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(cacheFile) // #nosec G304
}

// saveToCache saves data to cache
func (r *HTTPPluginRegistry) saveToCache(key string, data []byte) error {
	if r.cacheDir == "" {
		return fmt.Errorf("cache not configured")
	}

	if err := os.MkdirAll(r.cacheDir, common.DirPermissionNormal); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	cacheFile := filepath.Join(r.cacheDir, key)
	return os.WriteFile(cacheFile, data, 0600)
}

// NewLocalPluginRegistry creates a new local file system-based plugin registry
func NewLocalPluginRegistry(registryDir, cacheDir string) *LocalPluginRegistry {
	return &LocalPluginRegistry{
		registryDir: registryDir,
		cacheDir:    cacheDir,
	}
}

// List returns available plugins from the local registry
func (r *LocalPluginRegistry) List(ctx context.Context) ([]PluginManifest, error) {
	var plugins []PluginManifest

	err := filepath.Walk(r.registryDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, "plugin.yaml") {
			return nil
		}

		path, err = common.ValidatePath(path, r.registryDir)
		if err != nil {
			return fmt.Errorf("invalid registry path: %w", err)
		}
		data, err := os.ReadFile(path) // #nosec G304
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}

		var manifest PluginManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			// Try YAML if JSON fails
			return fmt.Errorf("failed to parse manifest %s: %w", path, err)
		}

		plugins = append(plugins, manifest)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan registry directory: %w", err)
	}

	// Sort plugins by name
	sort.Slice(plugins, func(i, j int) bool {
		return plugins[i].Name < plugins[j].Name
	})

	return plugins, nil
}

// Get retrieves a specific plugin manifest from local registry
func (r *LocalPluginRegistry) Get(ctx context.Context, name, version string) (*PluginManifest, error) {
	manifestPath := filepath.Join(r.registryDir, name, "plugin.yaml")
	
	manifestPath, err := common.ValidatePath(manifestPath, r.registryDir)
	if err != nil {
		return nil, fmt.Errorf("invalid manifest path: %w", err)
	}
	data, err := os.ReadFile(manifestPath) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("plugin %s not found", name)
		}
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest PluginManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	// Check version if specified
	if version != "" && manifest.Version != version {
		return nil, fmt.Errorf("version %s not found for plugin %s", version, name)
	}

	return &manifest, nil
}

// Search searches for plugins in the local registry
func (r *LocalPluginRegistry) Search(ctx context.Context, query string) ([]PluginManifest, error) {
	allPlugins, err := r.List(ctx)
	if err != nil {
		return nil, err
	}

	query = strings.ToLower(query)
	var matches []PluginManifest

	for _, plugin := range allPlugins {
		// Search in name, description, keywords, and author
		if r.matchesQuery(plugin, query) {
			matches = append(matches, plugin)
		}
	}

	return matches, nil
}

// matchesQuery checks if a plugin matches the search query
func (r *LocalPluginRegistry) matchesQuery(plugin PluginManifest, query string) bool {
	if strings.Contains(strings.ToLower(plugin.Name), query) {
		return true
	}
	
	if strings.Contains(strings.ToLower(plugin.Description), query) {
		return true
	}
	
	if strings.Contains(strings.ToLower(plugin.Author), query) {
		return true
	}

	for _, keyword := range plugin.Keywords {
		if strings.Contains(strings.ToLower(keyword), query) {
			return true
		}
	}

	return false
}

// Download downloads a plugin from the local registry
func (r *LocalPluginRegistry) Download(ctx context.Context, name, version string) ([]byte, error) {
	pluginDir := filepath.Join(r.registryDir, name)
	
	// Look for plugin binary/archive
	possibleExtensions := []string{".so", ".dll", ".dylib", ".wasm", ".tar.gz", ".zip"}
	
	for _, ext := range possibleExtensions {
		pluginPath := filepath.Join(pluginDir, name+ext)
		if _, err := os.Stat(pluginPath); err == nil {
			pluginPath, err = common.ValidatePath(pluginPath, r.registryDir)
			if err != nil {
				return nil, err
			}
			return os.ReadFile(pluginPath) // #nosec G304
		}
	}

	return nil, fmt.Errorf("plugin binary not found for %s", name)
}

// RegistryManager manages multiple plugin registries
type RegistryManager struct {
	registries []PluginRegistry
	primary    PluginRegistry
}

// NewRegistryManager creates a new registry manager
func NewRegistryManager() *RegistryManager {
	return &RegistryManager{
		registries: make([]PluginRegistry, 0),
	}
}

// AddRegistry adds a registry
func (rm *RegistryManager) AddRegistry(registry PluginRegistry) {
	rm.registries = append(rm.registries, registry)
	
	// Set first registry as primary
	if rm.primary == nil {
		rm.primary = registry
	}
}

// SetPrimary sets the primary registry
func (rm *RegistryManager) SetPrimary(registry PluginRegistry) {
	rm.primary = registry
}

// List returns plugins from all registries
func (rm *RegistryManager) List(ctx context.Context) ([]PluginManifest, error) {
	var allPlugins []PluginManifest
	seen := make(map[string]bool)

	for _, registry := range rm.registries {
		plugins, err := registry.List(ctx)
		if err != nil {
			// Log error but continue with other registries
			fmt.Printf("Warning: registry failed to list plugins: %v\n", err)
			continue
		}

		for _, plugin := range plugins {
			key := fmt.Sprintf("%s-%s", plugin.Name, plugin.Version)
			if !seen[key] {
				allPlugins = append(allPlugins, plugin)
				seen[key] = true
			}
		}
	}

	return allPlugins, nil
}

// Get retrieves a plugin from registries (tries primary first)
func (rm *RegistryManager) Get(ctx context.Context, name, version string) (*PluginManifest, error) {
	// Try primary registry first
	if rm.primary != nil {
		if manifest, err := rm.primary.Get(ctx, name, version); err == nil {
			return manifest, nil
		}
	}

	// Try other registries
	for _, registry := range rm.registries {
		if registry == rm.primary {
			continue // Already tried
		}

		if manifest, err := registry.Get(ctx, name, version); err == nil {
			return manifest, nil
		}
	}

	return nil, fmt.Errorf("plugin %s not found in any registry", name)
}

// Search searches all registries
func (rm *RegistryManager) Search(ctx context.Context, query string) ([]PluginManifest, error) {
	var allResults []PluginManifest
	seen := make(map[string]bool)

	for _, registry := range rm.registries {
		results, err := registry.Search(ctx, query)
		if err != nil {
			// Log error but continue with other registries
			fmt.Printf("Warning: registry search failed: %v\n", err)
			continue
		}

		for _, plugin := range results {
			key := fmt.Sprintf("%s-%s", plugin.Name, plugin.Version)
			if !seen[key] {
				allResults = append(allResults, plugin)
				seen[key] = true
			}
		}
	}

	return allResults, nil
}

// Download downloads a plugin (tries primary registry first)
func (rm *RegistryManager) Download(ctx context.Context, name, version string) ([]byte, error) {
	// Try primary registry first
	if rm.primary != nil {
		if data, err := rm.primary.Download(ctx, name, version); err == nil {
			return data, nil
		}
	}

	// Try other registries
	for _, registry := range rm.registries {
		if registry == rm.primary {
			continue // Already tried
		}

		if data, err := registry.Download(ctx, name, version); err == nil {
			return data, nil
		}
	}

	return nil, fmt.Errorf("failed to download plugin %s from any registry", name)
}