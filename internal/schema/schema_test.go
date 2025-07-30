package schema

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparator_CompareSchemas(t *testing.T) {
	tests := []struct {
		name           string
		sourceObjects  []SchemaObject
		targetObjects  []SchemaObject
		options        ComparisonOptions
		expectedDiffs  int
		expectedAdded  int
		expectedRemoved int
		expectedModified int
	}{
		{
			name: "identical schemas",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "view1", Type: ObjectTypeView, Database: "DB1", Schema: "SCHEMA1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "view1", Type: ObjectTypeView, Database: "DB1", Schema: "SCHEMA1"},
			},
			options:          ComparisonOptions{},
			expectedDiffs:    0,
			expectedAdded:    0,
			expectedRemoved:  0,
			expectedModified: 0,
		},
		{
			name: "object added in target",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "table2", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			options:          ComparisonOptions{},
			expectedDiffs:    1,
			expectedAdded:    1,
			expectedRemoved:  0,
			expectedModified: 0,
		},
		{
			name: "object removed from target",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "table2", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			options:          ComparisonOptions{},
			expectedDiffs:    1,
			expectedAdded:    0,
			expectedRemoved:  1,
			expectedModified: 0,
		},
		{
			name: "object modified",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER2"},
			},
			options:          ComparisonOptions{IgnoreOwner: false},
			expectedDiffs:    1,
			expectedAdded:    0,
			expectedRemoved:  0,
			expectedModified: 1,
		},
		{
			name: "owner change ignored",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER2"},
			},
			options:          ComparisonOptions{IgnoreOwner: true},
			expectedDiffs:    0,
			expectedAdded:    0,
			expectedRemoved:  0,
			expectedModified: 0,
		},
		{
			name: "filter by object type",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "view1", Type: ObjectTypeView, Database: "DB1", Schema: "SCHEMA1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			options:          ComparisonOptions{IncludeTypes: []ObjectType{ObjectTypeTable}},
			expectedDiffs:    0,
			expectedAdded:    0,
			expectedRemoved:  0,
			expectedModified: 0,
		},
		{
			name: "exclude pattern",
			sourceObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
				{Name: "temp_table", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			targetObjects: []SchemaObject{
				{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			},
			options:          ComparisonOptions{ExcludePatterns: []string{"^temp_.*"}},
			expectedDiffs:    0,
			expectedAdded:    0,
			expectedRemoved:  0,
			expectedModified: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comparator := NewComparator(nil, tt.options)
			
			sourceSnapshot := SchemaSnapshot{
				Environment: "source",
				Objects:     tt.sourceObjects,
				CapturedAt:  time.Now(),
			}
			
			targetSnapshot := SchemaSnapshot{
				Environment: "target",
				Objects:     tt.targetObjects,
				CapturedAt:  time.Now(),
			}

			result, err := comparator.CompareSchemas(context.Background(), sourceSnapshot, targetSnapshot)
			require.NoError(t, err)
			
			assert.Equal(t, tt.expectedDiffs, len(result.Differences))
			assert.Equal(t, tt.expectedAdded, result.Summary.AddedObjects)
			assert.Equal(t, tt.expectedRemoved, result.Summary.RemovedObjects)
			assert.Equal(t, tt.expectedModified, result.Summary.ModifiedObjects)
		})
	}
}

func TestSyncGenerator_GenerateSyncScript(t *testing.T) {
	tests := []struct {
		name        string
		result      ComparisonResult
		options     SyncOptions
		expectSQL   []string
		notExpectSQL []string
	}{
		{
			name: "generate create statements",
			result: ComparisonResult{
				SourceEnvironment: "dev",
				TargetEnvironment: "prod",
				Differences: []Difference{
					{
						ObjectName: "new_table",
						ObjectType: ObjectTypeTable,
						Database:   "DB1",
						Schema:     "SCHEMA1",
						DiffType:   DiffTypeRemoved,
						SourceObject: &SchemaObject{
							Name:       "new_table",
							Type:       ObjectTypeTable,
							Definition: "CREATE TABLE new_table (id INT);",
						},
					},
				},
			},
			options: SyncOptions{
				IncludeCreates: true,
				AddComments:    true,
			},
			expectSQL: []string{
				"CREATE TABLE new_table (id INT);",
			},
		},
		{
			name: "generate drop statements with safe mode",
			result: ComparisonResult{
				SourceEnvironment: "dev",
				TargetEnvironment: "prod",
				Differences: []Difference{
					{
						ObjectName: "old_table",
						ObjectType: ObjectTypeTable,
						Database:   "DB1",
						Schema:     "SCHEMA1",
						DiffType:   DiffTypeAdded,
						TargetObject: &SchemaObject{
							Name: "old_table",
							Type: ObjectTypeTable,
						},
					},
				},
			},
			options: SyncOptions{
				IncludeDrops: true,
				SafeMode:     true,
			},
			notExpectSQL: []string{
				"DROP TABLE",
			},
		},
		{
			name: "generate drop statements without safe mode",
			result: ComparisonResult{
				SourceEnvironment: "dev",
				TargetEnvironment: "prod",
				Differences: []Difference{
					{
						ObjectName: "old_table",
						ObjectType: ObjectTypeTable,
						Database:   "DB1",
						Schema:     "SCHEMA1",
						DiffType:   DiffTypeAdded,
						TargetObject: &SchemaObject{
							Name:     "old_table",
							Type:     ObjectTypeTable,
							Database: "DB1",
							Schema:   "SCHEMA1",
						},
					},
				},
			},
			options: SyncOptions{
				IncludeDrops: true,
				SafeMode:     false,
			},
			expectSQL: []string{
				"DROP TABLE IF EXISTS DB1.SCHEMA1.old_table CASCADE;",
			},
		},
		{
			name: "generate alter for comment change",
			result: ComparisonResult{
				SourceEnvironment: "dev",
				TargetEnvironment: "prod",
				Differences: []Difference{
					{
						ObjectName:   "table1",
						ObjectType:   ObjectTypeTable,
						Database:     "DB1",
						Schema:       "SCHEMA1",
						DiffType:     DiffTypeModified,
						SourceObject: &SchemaObject{Name: "table1", Database: "DB1", Schema: "SCHEMA1"},
						TargetObject: &SchemaObject{Name: "table1", Database: "DB1", Schema: "SCHEMA1"},
						Details: []DifferenceDetail{
							{
								Property:    "Comment",
								SourceValue: "Old comment",
								TargetValue: "New comment",
							},
						},
					},
				},
			},
			options: SyncOptions{
				IncludeAlters: true,
			},
			expectSQL: []string{
				"COMMENT ON TABLE DB1.SCHEMA1.table1 IS 'New comment';",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator := NewSyncGenerator(tt.options)
			script := generator.GenerateSyncScript(&tt.result)

			for _, expectedSQL := range tt.expectSQL {
				assert.Contains(t, script, expectedSQL)
			}

			for _, notExpectedSQL := range tt.notExpectSQL {
				assert.NotContains(t, script, notExpectedSQL)
			}
		})
	}
}

func TestReporter_GenerateReport(t *testing.T) {
	result := &ComparisonResult{
		SourceEnvironment: "dev",
		TargetEnvironment: "prod",
		ComparedAt:        time.Now(),
		Summary: ComparisonSummary{
			TotalObjects:     10,
			MatchingObjects:  7,
			DifferentObjects: 3,
			AddedObjects:     1,
			RemovedObjects:   1,
			ModifiedObjects:  1,
			ByObjectType: map[ObjectType]ObjectTypeSummary{
				ObjectTypeTable: {
					Total:     5,
					Matching:  3,
					Different: 2,
					Added:     1,
					Modified:  1,
				},
			},
		},
		Differences: []Difference{
			{
				ObjectName:  "new_table",
				ObjectType:  ObjectTypeTable,
				Database:    "DB1",
				Schema:      "SCHEMA1",
				DiffType:    DiffTypeAdded,
				Description: "Table exists in prod but not in dev",
			},
		},
	}

	reporter := NewReporter(result)

	formats := []ReportFormat{
		ReportFormatText,
		ReportFormatJSON,
		ReportFormatMarkdown,
		ReportFormatCSV,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			report, err := reporter.GenerateReport(format)
			require.NoError(t, err)
			assert.NotEmpty(t, report.Content)
			assert.Equal(t, format, report.Format)
		})
	}
}

func TestVersionStore_CaptureAndLoad(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	store := NewVersionStore(tmpDir, nil)

	// Create test version
	version := &SchemaVersion{
		Version:     "test123",
		Environment: "test",
		CapturedAt:  time.Now(),
		CapturedBy:  "test_user",
		Description: "Test version",
		Objects: []SchemaObject{
			{Name: "table1", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			{Name: "view1", Type: ObjectTypeView, Database: "DB1", Schema: "SCHEMA1"},
		},
		Metadata: map[string]interface{}{
			"test": "value",
		},
	}

	// Save version
	err := store.SaveVersion(version)
	require.NoError(t, err)

	// Load version
	loaded, err := store.LoadVersion("test", "test123")
	require.NoError(t, err)
	
	assert.Equal(t, version.Version, loaded.Version)
	assert.Equal(t, version.Environment, loaded.Environment)
	assert.Equal(t, len(version.Objects), len(loaded.Objects))
	assert.Equal(t, version.Description, loaded.Description)

	// Load latest version
	latest, err := store.LoadLatestVersion("test")
	require.NoError(t, err)
	assert.Equal(t, version.Version, latest.Version)

	// List versions
	versions, err := store.ListVersions("test")
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, version.Version, versions[0].Version)
}

func TestVisualizer_DisplaySideBySide(t *testing.T) {
	visualizer := NewVisualizer(false) // No color for testing

	diff := Difference{
		ObjectName:  "table1",
		ObjectType:  ObjectTypeTable,
		Database:    "DB1",
		Schema:      "SCHEMA1",
		DiffType:    DiffTypeModified,
		SourceObject: &SchemaObject{
			Name:    "table1",
			Type:    ObjectTypeTable,
			Owner:   "USER1",
			Comment: "Original table",
		},
		TargetObject: &SchemaObject{
			Name:    "table1",
			Type:    ObjectTypeTable,
			Owner:   "USER2",
			Comment: "Modified table",
		},
		Details: []DifferenceDetail{
			{
				Property:    "Owner",
				SourceValue: "USER1",
				TargetValue: "USER2",
			},
			{
				Property:    "Comment",
				SourceValue: "Original table",
				TargetValue: "Modified table",
			},
		},
	}

	output := visualizer.DisplaySideBySide(diff)
	assert.Contains(t, output, "table1")
	assert.Contains(t, output, "SOURCE")
	assert.Contains(t, output, "TARGET")
	assert.Contains(t, output, "USER1")
	assert.Contains(t, output, "USER2")
}

func TestComparator_IgnoreRules(t *testing.T) {
	comparator := NewComparator(nil, ComparisonOptions{})
	
	// Add ignore rule for temp tables
	comparator.AddIgnoreRule(IgnoreRule{
		Name:        "ignore_temp_tables",
		ObjectType:  ObjectTypeTable,
		Pattern:     "^TEMP_.*",
		Description: "Ignore temporary tables",
	})

	// Add ignore rule for owner changes
	comparator.AddIgnoreRule(IgnoreRule{
		Name:        "ignore_owner_changes",
		Property:    "Owner",
		Description: "Ignore owner changes",
	})

	sourceSnapshot := SchemaSnapshot{
		Environment: "source",
		Objects: []SchemaObject{
			{Name: "TEMP_TABLE", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1"},
			{Name: "REAL_TABLE", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER1"},
		},
		CapturedAt: time.Now(),
	}

	targetSnapshot := SchemaSnapshot{
		Environment: "target",
		Objects: []SchemaObject{
			{Name: "REAL_TABLE", Type: ObjectTypeTable, Database: "DB1", Schema: "SCHEMA1", Owner: "USER2"},
		},
		CapturedAt: time.Now(),
	}

	result, err := comparator.CompareSchemas(context.Background(), sourceSnapshot, targetSnapshot)
	require.NoError(t, err)

	// Should not report the TEMP_TABLE as removed (ignored by pattern)
	// Should not report the owner change (ignored by property rule)
	assert.Equal(t, 0, len(result.Differences))
}