package common

// File permission constants for consistent security across the application
const (
	// FilePermissionSecure is used for sensitive files (config, credentials, keys, etc.)
	FilePermissionSecure = 0600
	
	// FilePermissionNormal is used for non-sensitive files (documentation, generated reports, etc.)
	FilePermissionNormal = 0644
	
	// FilePermissionExecutable is used for executable scripts
	FilePermissionExecutable = 0755
	
	// DirPermissionSecure is used for directories containing sensitive files
	DirPermissionSecure = 0700
	
	// DirPermissionNormal is used for normal directories
	DirPermissionNormal = 0755
)