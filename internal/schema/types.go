package schema

import (
	"time"
)

// ObjectType represents the type of database object
type ObjectType string

const (
	ObjectTypeTable      ObjectType = "TABLE"
	ObjectTypeView       ObjectType = "VIEW"
	ObjectTypeProcedure  ObjectType = "PROCEDURE"
	ObjectTypeFunction   ObjectType = "FUNCTION"
	ObjectTypeSequence   ObjectType = "SEQUENCE"
	ObjectTypeStage      ObjectType = "STAGE"
	ObjectTypePipe       ObjectType = "PIPE"
	ObjectTypeStream     ObjectType = "STREAM"
	ObjectTypeTask       ObjectType = "TASK"
	ObjectTypeFileFormat ObjectType = "FILE_FORMAT"
)

// SchemaObject represents a database object
type SchemaObject struct {
	Name         string
	Type         ObjectType
	Database     string
	Schema       string
	Owner        string
	CreatedOn    time.Time
	LastModified time.Time
	Comment      string
	Definition   string
	Metadata     map[string]interface{}
}

// Column represents a table column
type Column struct {
	Name         string
	Type         string
	Nullable     bool
	Default      *string
	PrimaryKey   bool
	UniqueKey    bool
	ForeignKey   *ForeignKey
	Comment      string
	CollationType string
	Position     int
}

// ForeignKey represents a foreign key constraint
type ForeignKey struct {
	Name               string
	ReferencedDatabase string
	ReferencedSchema   string
	ReferencedTable    string
	ReferencedColumn   string
	OnDelete           string
	OnUpdate           string
}

// TableDetails contains detailed information about a table
type TableDetails struct {
	SchemaObject
	Columns     []Column
	Indexes     []Index
	Constraints []Constraint
	ClusterKeys []string
	RowCount    int64
	SizeBytes   int64
}

// Index represents a table index
type Index struct {
	Name       string
	Columns    []string
	Type       string
	Unique     bool
	Clustered  bool
	Expression string
}

// Constraint represents a table constraint
type Constraint struct {
	Name       string
	Type       string // CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY
	Columns    []string
	Expression string
	Enabled    bool
}

// ViewDetails contains detailed information about a view
type ViewDetails struct {
	SchemaObject
	IsSecure     bool
	IsMaterialized bool
	ViewDefinition string
}

// ProcedureDetails contains detailed information about a stored procedure
type ProcedureDetails struct {
	SchemaObject
	Language    string
	Arguments   []ProcedureArgument
	ReturnType  string
	Body        string
	IsSecure    bool
}

// ProcedureArgument represents a procedure argument
type ProcedureArgument struct {
	Name     string
	Type     string
	Mode     string // IN, OUT, INOUT
	Default  *string
	Position int
}

// FunctionDetails contains detailed information about a function
type FunctionDetails struct {
	SchemaObject
	Language    string
	Arguments   []FunctionArgument
	ReturnType  string
	Body        string
	IsSecure    bool
	IsAggregate bool
}

// FunctionArgument represents a function argument
type FunctionArgument struct {
	Name     string
	Type     string
	Default  *string
	Position int
}

// ComparisonResult represents the result of comparing schemas
type ComparisonResult struct {
	SourceEnvironment string
	TargetEnvironment string
	ComparedAt        time.Time
	Differences       []Difference
	Summary           ComparisonSummary
	SyncScript        string
}

// Difference represents a difference between two schema objects
type Difference struct {
	ObjectName   string
	ObjectType   ObjectType
	Database     string
	Schema       string
	DiffType     DifferenceType
	Description  string
	SourceObject *SchemaObject
	TargetObject *SchemaObject
	Details      []DifferenceDetail
	SyncSQL      string
}

// DifferenceType represents the type of difference
type DifferenceType string

const (
	DiffTypeAdded    DifferenceType = "ADDED"
	DiffTypeRemoved  DifferenceType = "REMOVED"
	DiffTypeModified DifferenceType = "MODIFIED"
)

// DifferenceDetail represents a detailed difference within an object
type DifferenceDetail struct {
	Property    string
	SourceValue interface{}
	TargetValue interface{}
	Description string
}

// ComparisonSummary provides a summary of comparison results
type ComparisonSummary struct {
	TotalObjects      int
	MatchingObjects   int
	DifferentObjects  int
	AddedObjects      int
	RemovedObjects    int
	ModifiedObjects   int
	ByObjectType      map[ObjectType]ObjectTypeSummary
}

// ObjectTypeSummary provides summary for a specific object type
type ObjectTypeSummary struct {
	Total     int
	Matching  int
	Different int
	Added     int
	Removed   int
	Modified  int
}

// ComparisonOptions contains options for schema comparison
type ComparisonOptions struct {
	IncludeTypes      []ObjectType
	ExcludeTypes      []ObjectType
	IncludePatterns   []string
	ExcludePatterns   []string
	IgnoreWhitespace  bool
	IgnoreComments    bool
	IgnoreColumnOrder bool
	IgnoreOwner       bool
	IgnoreGrants      bool
	CaseSensitive     bool
	DeepCompare       bool
}

// SyncOptions contains options for generating sync scripts
type SyncOptions struct {
	IncludeDrops      bool
	IncludeCreates    bool
	IncludeAlters     bool
	UseTransactions   bool
	GenerateBackup    bool
	AddComments       bool
	ObjectOrder       []ObjectType
	SafeMode          bool // Don't drop objects that exist only in target
}

// ComparisonReport represents a formatted comparison report
type ComparisonReport struct {
	Result     ComparisonResult
	Format     ReportFormat
	Content    string
	Sections   []ReportSection
	GeneratedAt time.Time
}

// ReportFormat represents the format of the comparison report
type ReportFormat string

const (
	ReportFormatText     ReportFormat = "text"
	ReportFormatHTML     ReportFormat = "html"
	ReportFormatJSON     ReportFormat = "json"
	ReportFormatMarkdown ReportFormat = "markdown"
	ReportFormatCSV      ReportFormat = "csv"
)

// ReportSection represents a section in the comparison report
type ReportSection struct {
	Title   string
	Content string
	Level   int
}

// SchemaVersion represents a version of a schema
type SchemaVersion struct {
	Version      string
	Environment  string
	CapturedAt   time.Time
	CapturedBy   string
	Description  string
	Objects      []SchemaObject
	Metadata     map[string]interface{}
}

// IgnoreRule represents a rule for ignoring certain differences
type IgnoreRule struct {
	Name        string
	ObjectType  ObjectType
	Pattern     string
	Property    string
	Condition   string
	Description string
}