package rollback

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"flakedrop/internal/common"
	"flakedrop/pkg/errors"
)

// HistoryManager manages deployment history and versioning
type HistoryManager struct {
	storageDir    string
	mu            sync.RWMutex
	deployments   map[string]*DeploymentRecord
	maxHistory    int
	retentionDays int
}

// NewHistoryManager creates a new history manager
func NewHistoryManager(storageDir string) (*HistoryManager, error) {
	if err := os.MkdirAll(storageDir, common.DirPermissionNormal); err != nil {
		return nil, fmt.Errorf("failed to create history directory: %w", err)
	}

	hm := &HistoryManager{
		storageDir:    storageDir,
		deployments:   make(map[string]*DeploymentRecord),
		maxHistory:    100,
		retentionDays: 30,
	}

	// Load existing history
	if err := hm.loadHistory(); err != nil {
		return nil, fmt.Errorf("failed to load history: %w", err)
	}

	return hm, nil
}

// RecordDeployment records a new deployment
func (hm *HistoryManager) RecordDeployment(deployment *DeploymentRecord) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Set previous deployment ID
	if prev := hm.getLatestDeployment(deployment.Database, deployment.Schema); prev != nil {
		deployment.PreviousID = prev.ID
	}

	// Store in memory
	hm.deployments[deployment.ID] = deployment

	// Persist to disk
	if err := hm.saveDeployment(deployment); err != nil {
		return fmt.Errorf("failed to save deployment: %w", err)
	}

	// Clean up old deployments
	hm.cleanupOldDeployments()

	return nil
}

// UpdateDeployment updates an existing deployment record
func (hm *HistoryManager) UpdateDeployment(id string, update func(*DeploymentRecord)) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	deployment, exists := hm.deployments[id]
	if !exists {
		return errors.New(errors.ErrCodeNotFound, "deployment not found").
			WithContext("deployment_id", id)
	}

	// Apply update
	update(deployment)

	// Persist changes
	if err := hm.saveDeployment(deployment); err != nil {
		return fmt.Errorf("failed to save deployment update: %w", err)
	}

	return nil
}

// GetDeployment retrieves a deployment by ID
func (hm *HistoryManager) GetDeployment(id string) (*DeploymentRecord, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	deployment, exists := hm.deployments[id]
	if !exists {
		return nil, errors.New(errors.ErrCodeNotFound, "deployment not found").
			WithContext("deployment_id", id)
	}

	return deployment, nil
}

// GetDeploymentHistory retrieves deployment history for a database/schema
func (hm *HistoryManager) GetDeploymentHistory(database, schema string, limit int) ([]*DeploymentRecord, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var history []*DeploymentRecord

	for _, deployment := range hm.deployments {
		if deployment.Database == database && deployment.Schema == schema {
			history = append(history, deployment)
		}
	}

	// Sort by start time (newest first)
	sort.Slice(history, func(i, j int) bool {
		return history[i].StartTime.After(history[j].StartTime)
	})

	// Apply limit
	if limit > 0 && len(history) > limit {
		history = history[:limit]
	}

	return history, nil
}

// GetSuccessfulDeployments retrieves successful deployments
func (hm *HistoryManager) GetSuccessfulDeployments(database, schema string, since time.Time) ([]*DeploymentRecord, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var deployments []*DeploymentRecord

	for _, deployment := range hm.deployments {
		if deployment.Database == database && 
		   deployment.Schema == schema && 
		   deployment.State == StateCompleted &&
		   deployment.StartTime.After(since) {
			deployments = append(deployments, deployment)
		}
	}

	// Sort by start time
	sort.Slice(deployments, func(i, j int) bool {
		return deployments[i].StartTime.After(deployments[j].StartTime)
	})

	return deployments, nil
}

// GetLatestSuccessfulDeployment gets the most recent successful deployment
func (hm *HistoryManager) GetLatestSuccessfulDeployment(database, schema string) (*DeploymentRecord, error) {
	deployments, err := hm.GetSuccessfulDeployments(database, schema, time.Time{})
	if err != nil {
		return nil, err
	}

	if len(deployments) == 0 {
		return nil, errors.New(errors.ErrCodeNotFound, "no successful deployments found").
			WithContext("database", database).
			WithContext("schema", schema)
	}

	return deployments[0], nil
}

// GetDeploymentChain retrieves the chain of deployments from current to target
func (hm *HistoryManager) GetDeploymentChain(currentID, targetID string) ([]*DeploymentRecord, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var chain []*DeploymentRecord
	visited := make(map[string]bool)

	// Build chain from current to target
	current := currentID
	for current != "" && current != targetID {
		if visited[current] {
			return nil, errors.New(errors.ErrCodeInternal, "circular reference in deployment chain")
		}
		visited[current] = true

		deployment, exists := hm.deployments[current]
		if !exists {
			return nil, errors.New(errors.ErrCodeNotFound, "deployment not found in chain").
				WithContext("deployment_id", current)
		}

		chain = append(chain, deployment)
		current = deployment.PreviousID
	}

	// Add target deployment
	if current == targetID && targetID != "" {
		if deployment, exists := hm.deployments[targetID]; exists {
			chain = append(chain, deployment)
		}
	}

	return chain, nil
}

// ExportHistory exports deployment history to a file
func (hm *HistoryManager) ExportHistory(ctx context.Context, outputPath string, filter func(*DeploymentRecord) bool) error {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var deployments []*DeploymentRecord

	for _, deployment := range hm.deployments {
		if filter == nil || filter(deployment) {
			deployments = append(deployments, deployment)
		}
	}

	// Sort by start time
	sort.Slice(deployments, func(i, j int) bool {
		return deployments[i].StartTime.Before(deployments[j].StartTime)
	})

	// Validate the output path
	validatedPath, err := common.CleanPath(outputPath)
	if err != nil {
		return fmt.Errorf("invalid output path: %w", err)
	}
	
	// Export to file
	file, err := os.Create(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return fmt.Errorf("failed to create export file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	
	if err := encoder.Encode(deployments); err != nil {
		return fmt.Errorf("failed to encode deployments: %w", err)
	}

	return nil
}

// Private methods

func (hm *HistoryManager) loadHistory() error {
	pattern := filepath.Join(hm.storageDir, "deployment-*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, file := range files {
		deployment, err := hm.loadDeployment(file)
		if err != nil {
			// Log error but continue loading
			continue
		}
		hm.deployments[deployment.ID] = deployment
	}

	return nil
}

func (hm *HistoryManager) saveDeployment(deployment *DeploymentRecord) error {
	filename := fmt.Sprintf("deployment-%s.json", deployment.ID)
	filepath := filepath.Join(hm.storageDir, filename)

	data, err := json.MarshalIndent(deployment, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath, data, 0600)
}

func (hm *HistoryManager) loadDeployment(filepath string) (*DeploymentRecord, error) {
	// Validate the file path
	validatedPath, err := common.ValidatePath(filepath, hm.storageDir)
	if err != nil {
		return nil, fmt.Errorf("invalid file path: %w", err)
	}
	
	data, err := os.ReadFile(validatedPath) // #nosec G304 - path is validated
	if err != nil {
		return nil, err
	}

	var deployment DeploymentRecord
	if err := json.Unmarshal(data, &deployment); err != nil {
		return nil, err
	}

	return &deployment, nil
}

func (hm *HistoryManager) getLatestDeployment(database, schema string) *DeploymentRecord {
	var latest *DeploymentRecord
	
	for _, deployment := range hm.deployments {
		if deployment.Database == database && deployment.Schema == schema {
			if latest == nil || deployment.StartTime.After(latest.StartTime) {
				latest = deployment
			}
		}
	}

	return latest
}

func (hm *HistoryManager) cleanupOldDeployments() {
	cutoff := time.Now().AddDate(0, 0, -hm.retentionDays)
	
	for id, deployment := range hm.deployments {
		if deployment.StartTime.Before(cutoff) {
			// Remove from memory
			delete(hm.deployments, id)
			
			// Remove from disk
			filename := fmt.Sprintf("deployment-%s.json", id)
			filepath := filepath.Join(hm.storageDir, filename)
			os.Remove(filepath)
		}
	}

	// Keep only maxHistory deployments per database/schema
	deploymentsBySchema := make(map[string][]*DeploymentRecord)
	
	for _, deployment := range hm.deployments {
		key := fmt.Sprintf("%s.%s", deployment.Database, deployment.Schema)
		deploymentsBySchema[key] = append(deploymentsBySchema[key], deployment)
	}

	for _, deployments := range deploymentsBySchema {
		if len(deployments) > hm.maxHistory {
			// Sort by start time (oldest first)
			sort.Slice(deployments, func(i, j int) bool {
				return deployments[i].StartTime.Before(deployments[j].StartTime)
			})

			// Remove oldest deployments
			toRemove := len(deployments) - hm.maxHistory
			for i := 0; i < toRemove; i++ {
				id := deployments[i].ID
				delete(hm.deployments, id)
				
				filename := fmt.Sprintf("deployment-%s.json", id)
				filepath := filepath.Join(hm.storageDir, filename)
				os.Remove(filepath)
			}
		}
	}
}