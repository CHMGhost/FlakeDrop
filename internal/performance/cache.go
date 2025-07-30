package performance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Cache provides an in-memory cache with TTL and LRU eviction
type Cache struct {
	mu          sync.RWMutex
	items       map[string]*CacheItem
	lru         *lruList
	maxSize     int
	maxMemory   int64
	currentMem  int64
	ttl         time.Duration
	stats       *CacheStats
	evictChan   chan string
	stopCleanup chan struct{}
}

// CacheItem represents a single cached item
type CacheItem struct {
	Key        string
	Value      interface{}
	Size       int64
	CreatedAt  time.Time
	AccessedAt time.Time
	AccessCount int64
	TTL        time.Duration
	node       *lruNode
}

// CacheStats tracks cache performance metrics
type CacheStats struct {
	mu          sync.RWMutex
	Hits        int64
	Misses      int64
	Evictions   int64
	Expired     int64
	TotalSize   int64
	ItemCount   int64
}

// lruList implements a doubly linked list for LRU eviction
type lruList struct {
	head *lruNode
	tail *lruNode
	size int
}

type lruNode struct {
	key  string
	prev *lruNode
	next *lruNode
}

// CacheConfig contains cache configuration
type CacheConfig struct {
	MaxItems    int
	MaxMemoryMB int64
	DefaultTTL  time.Duration
	CleanupInterval time.Duration
}

// DefaultCacheConfig returns sensible cache defaults
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxItems:    10000,
		MaxMemoryMB: 100,
		DefaultTTL:  10 * time.Minute,
		CleanupInterval: 1 * time.Minute,
	}
}

// NewCache creates a new cache instance
func NewCache(config CacheConfig) *Cache {
	// Set default cleanup interval if not provided
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 1 * time.Minute
	}

	cache := &Cache{
		items:       make(map[string]*CacheItem),
		lru:         &lruList{},
		maxSize:     config.MaxItems,
		maxMemory:   config.MaxMemoryMB * 1024 * 1024,
		ttl:         config.DefaultTTL,
		stats:       &CacheStats{},
		evictChan:   make(chan string, 100),
		stopCleanup: make(chan struct{}),
	}

	// Start cleanup routine
	go cache.cleanupRoutine(config.CleanupInterval)

	return cache
}

// Get retrieves an item from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	item, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		c.stats.recordMiss()
		return nil, false
	}

	// Check if expired
	if item.isExpired() {
		c.mu.Lock()
		c.removeItem(key)
		c.mu.Unlock()
		c.stats.recordExpired()
		c.stats.recordMiss()
		return nil, false
	}

	// Update access time and count
	c.mu.Lock()
	item.AccessedAt = time.Now()
	item.AccessCount++
	c.lru.moveToFront(item.node)
	c.mu.Unlock()

	c.stats.recordHit()
	return item.Value, true
}

// Set adds or updates an item in the cache
func (c *Cache) Set(key string, value interface{}, size int64) error {
	return c.SetWithTTL(key, value, size, c.ttl)
}

// SetWithTTL adds or updates an item with a specific TTL
func (c *Cache) SetWithTTL(key string, value interface{}, size int64, ttl time.Duration) error {
	if size > c.maxMemory {
		return fmt.Errorf("item size %d exceeds max memory %d", size, c.maxMemory)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key already exists
	if existingItem, exists := c.items[key]; exists {
		// Update existing item
		c.currentMem -= existingItem.Size
		existingItem.Value = value
		existingItem.Size = size
		existingItem.AccessedAt = time.Now()
		existingItem.AccessCount++
		existingItem.TTL = ttl
		c.currentMem += size
		c.lru.moveToFront(existingItem.node)
		return nil
	}

	// Evict items if necessary
	for (len(c.items) >= c.maxSize || c.currentMem+size > c.maxMemory) && c.lru.size > 0 {
		evictKey := c.lru.removeTail()
		c.removeItem(evictKey)
		c.stats.recordEviction()
	}

	// Create new item
	item := &CacheItem{
		Key:         key,
		Value:       value,
		Size:        size,
		CreatedAt:   time.Now(),
		AccessedAt:  time.Now(),
		AccessCount: 1,
		TTL:         ttl,
	}

	// Add to LRU
	item.node = c.lru.addToFront(key)

	// Store item
	c.items[key] = item
	c.currentMem += size
	c.stats.updateSize(int64(len(c.items)), c.currentMem)

	return nil
}

// Delete removes an item from the cache
func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		c.removeItem(key)
		return true
	}
	return false
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*CacheItem)
	c.lru = &lruList{}
	c.currentMem = 0
	c.stats.updateSize(0, 0)
}

// GetStats returns cache statistics
func (c *Cache) GetStats() CacheStats {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()
	return *c.stats
}

// Stop stops the cache cleanup routine
func (c *Cache) Stop() {
	close(c.stopCleanup)
}

// removeItem removes an item without locking (caller must hold lock)
func (c *Cache) removeItem(key string) {
	if item, exists := c.items[key]; exists {
		c.lru.remove(item.node)
		c.currentMem -= item.Size
		delete(c.items, key)
		c.stats.updateSize(int64(len(c.items)), c.currentMem)
	}
}

// cleanupRoutine periodically removes expired items
func (c *Cache) cleanupRoutine(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes all expired items
func (c *Cache) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var toRemove []string
	for key, item := range c.items {
		if item.isExpired() {
			toRemove = append(toRemove, key)
		}
	}

	for _, key := range toRemove {
		c.removeItem(key)
		c.stats.recordExpired()
	}
}

// CacheItem methods

func (ci *CacheItem) isExpired() bool {
	if ci.TTL == 0 {
		return false
	}
	return time.Since(ci.CreatedAt) > ci.TTL
}

// LRU list methods

func (l *lruList) addToFront(key string) *lruNode {
	node := &lruNode{key: key}
	
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	
	l.size++
	return node
}

func (l *lruList) moveToFront(node *lruNode) {
	if node == l.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == l.tail {
		l.tail = node.prev
	}

	// Move to front
	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}

func (l *lruList) remove(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}

	l.size--
}

func (l *lruList) removeTail() string {
	if l.tail == nil {
		return ""
	}

	key := l.tail.key
	l.remove(l.tail)
	return key
}

// CacheStats methods

func (cs *CacheStats) recordHit() {
	cs.mu.Lock()
	cs.Hits++
	cs.mu.Unlock()
}

func (cs *CacheStats) recordMiss() {
	cs.mu.Lock()
	cs.Misses++
	cs.mu.Unlock()
}

func (cs *CacheStats) recordEviction() {
	cs.mu.Lock()
	cs.Evictions++
	cs.mu.Unlock()
}

func (cs *CacheStats) recordExpired() {
	cs.mu.Lock()
	cs.Expired++
	cs.mu.Unlock()
}

func (cs *CacheStats) updateSize(items, memory int64) {
	cs.mu.Lock()
	cs.ItemCount = items
	cs.TotalSize = memory
	cs.mu.Unlock()
}

func (cs *CacheStats) GetHitRate() float64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	
	total := cs.Hits + cs.Misses
	if total == 0 {
		return 0
	}
	return float64(cs.Hits) / float64(total) * 100
}

// QueryCache provides caching for SQL query results
type QueryCache struct {
	cache *Cache
}

// NewQueryCache creates a new query cache
func NewQueryCache(config CacheConfig) *QueryCache {
	return &QueryCache{
		cache: NewCache(config),
	}
}

// GetQuery retrieves a cached query result
func (qc *QueryCache) GetQuery(query string, params ...interface{}) (interface{}, bool) {
	key := qc.generateKey(query, params...)
	return qc.cache.Get(key)
}

// SetQuery caches a query result
func (qc *QueryCache) SetQuery(query string, result interface{}, size int64, params ...interface{}) error {
	key := qc.generateKey(query, params...)
	return qc.cache.Set(key, result, size)
}

// InvalidatePattern invalidates all cached queries matching a pattern
func (qc *QueryCache) InvalidatePattern(pattern string) int {
	// This is a simplified implementation
	// In production, you might want to use a more sophisticated pattern matching
	count := 0
	qc.cache.mu.Lock()
	defer qc.cache.mu.Unlock()

	for key := range qc.cache.items {
		if matchesPattern(key, pattern) {
			qc.cache.removeItem(key)
			count++
		}
	}
	return count
}

// generateKey creates a cache key from query and parameters
func (qc *QueryCache) generateKey(query string, params ...interface{}) string {
	h := sha256.New()
	h.Write([]byte(query))
	for _, param := range params {
		h.Write([]byte(fmt.Sprintf("%v", param)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// matchesPattern checks if a key matches a simple pattern
func matchesPattern(key, pattern string) bool {
	// Simple pattern matching - in production use regexp or glob
	return len(key) >= len(pattern) && key[:len(pattern)] == pattern
}

// FileCache provides caching for file contents
type FileCache struct {
	cache *Cache
}

// NewFileCache creates a new file cache
func NewFileCache(config CacheConfig) *FileCache {
	return &FileCache{
		cache: NewCache(config),
	}
}

// GetFile retrieves a cached file
func (fc *FileCache) GetFile(path string) (string, bool) {
	value, exists := fc.cache.Get(path)
	if !exists {
		return "", false
	}
	content, ok := value.(string)
	return content, ok
}

// SetFile caches file content
func (fc *FileCache) SetFile(path string, content string) error {
	size := int64(len(content))
	return fc.cache.Set(path, content, size)
}

// InvalidateFile removes a file from cache
func (fc *FileCache) InvalidateFile(path string) bool {
	return fc.cache.Delete(path)
}

// GetStats returns cache statistics
func (fc *FileCache) GetStats() CacheStats {
	return fc.cache.GetStats()
}