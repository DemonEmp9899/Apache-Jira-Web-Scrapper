"""
Configuration file for Jira Scraper
Modify these settings to customize scraper behavior
"""

# ==================== PROJECT SELECTION ====================
# Choose any 3 Apache projects to scrape
# Popular options: KAFKA, SPARK, HADOOP, CASSANDRA, FLINK, AIRFLOW, 
#                  BEAM, CAMEL, HIVE, HBASE, STORM, ZOOKEEPER
PROJECTS = ['KAFKA', 'BEAM', 'HARMONY']


# ==================== RATE LIMITING ====================
# Delay between requests in seconds
# Increase if you're getting rate limited
# Decrease for faster scraping (not recommended)
RATE_LIMIT_DELAY = 1.0  # seconds


# ==================== RETRY CONFIGURATION ====================
# Maximum number of retries for failed requests
MAX_RETRIES = 5

# Request timeout in seconds
REQUEST_TIMEOUT = 30


# ==================== CHECKPOINT SETTINGS ====================
# Checkpoint file location
CHECKPOINT_FILE = 'checkpoint.json'

# How often to save checkpoint (number of issues)
# Smaller = more frequent saves, larger = better performance
CHECKPOINT_FREQUENCY = 50


# ==================== OUTPUT SETTINGS ====================
# Output directory for JSONL files
OUTPUT_DIR = 'output'

# Whether to append to existing files or overwrite
APPEND_MODE = True


# ==================== LOGGING ====================
# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = 'INFO'

# Log file location
LOG_FILE = 'scraper.log'


# ==================== API SETTINGS ====================
# Jira API base URL
JIRA_BASE_URL = 'https://issues.apache.org/jira/rest/api/2'

# Fields to fetch from API (comma-separated)
# Only change if you know what you're doing
JIRA_FIELDS = (
    'summary,description,status,priority,issuetype,reporter,'
    'assignee,created,updated,resolutiondate,labels,components'
)


# ==================== DATA TRANSFORMATION ====================
# Minimum description length for summarization task
MIN_DESCRIPTION_LENGTH_FOR_SUMMARIZATION = 500

# Minimum comments for question_answering task
MIN_COMMENTS_FOR_QA = 2


# ==================== ADVANCED SETTINGS ====================
# Enable debug mode (more verbose logging)
DEBUG_MODE = False

# Test mode: only scrape first N issues per project
TEST_MODE = True
TEST_MODE_LIMIT = 10  # only used if TEST_MODE = True


# ==================== VALIDATION ====================
def validate_config():
    """Validate configuration settings"""
    errors = []
    
    if not PROJECTS or len(PROJECTS) == 0:
        errors.append("PROJECTS cannot be empty")
    
    if RATE_LIMIT_DELAY < 0:
        errors.append("RATE_LIMIT_DELAY must be positive")
    
    if MAX_RETRIES < 1:
        errors.append("MAX_RETRIES must be at least 1")
    
    if REQUEST_TIMEOUT < 1:
        errors.append("REQUEST_TIMEOUT must be at least 1")
    
    if errors:
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    return True


# Validate on import
validate_config()
