"""
Apache Jira Web Scraper
A fault-tolerant, resumable scraper for Apache Jira projects
"""

import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class JiraIssue:
    """Structured representation of a Jira issue"""
    issue_id: str
    project: str
    title: str
    description: str
    status: str
    priority: str
    issue_type: str
    reporter: str
    assignee: Optional[str]
    created_date: str
    updated_date: str
    resolved_date: Optional[str]
    labels: List[str]
    components: List[str]
    comments: List[Dict[str, str]]
    training_task: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


class JiraAPIClient:
    """Handles API communication with Apache Jira"""
    
    BASE_URL = "https://issues.apache.org/jira/rest/api/2"
    MAX_RESULTS = 50  # Jira's default pagination size
    
    def __init__(self, rate_limit_delay: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Apache-Jira-Scraper/1.0'
        })
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
        
    def _make_request(self, url: str, params: Optional[Dict] = None, 
                      max_retries: int = 5) -> Optional[Dict]:
        """Make HTTP request with retry logic and exponential backoff"""
        self._rate_limit()
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors
                if response.status_code >= 500:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Server error {response.status_code}. "
                                 f"Retry {attempt+1}/{max_retries} in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                
                # Success
                if response.status_code == 200:
                    return response.json()
                
                # Client error (non-recoverable)
                logger.error(f"Client error {response.status_code}: {response.text}")
                return None
                
            except requests.exceptions.Timeout:
                wait_time = 2 ** attempt
                logger.warning(f"Timeout. Retry {attempt+1}/{max_retries} in {wait_time}s")
                time.sleep(wait_time)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
        
        logger.error(f"Max retries reached for {url}")
        return None
    
    def search_issues(self, project: str, start_at: int = 0) -> Optional[Dict]:
        """Search for issues in a project with pagination"""
        url = f"{self.BASE_URL}/search"
        params = {
            'jql': f'project={project} ORDER BY created ASC',
            'startAt': start_at,
            'maxResults': self.MAX_RESULTS,
            'fields': 'summary,description,status,priority,issuetype,reporter,'
                     'assignee,created,updated,resolutiondate,labels,components'
        }
        return self._make_request(url, params)
    
    def get_issue_comments(self, issue_key: str) -> List[Dict]:
        """Fetch all comments for an issue"""
        url = f"{self.BASE_URL}/issue/{issue_key}/comment"
        data = self._make_request(url)
        
        if not data or 'comments' not in data:
            return []
        
        comments = []
        for comment in data['comments']:
            comments.append({
                'author': comment.get('author', {}).get('displayName', 'Unknown'),
                'created': comment.get('created', ''),
                'body': comment.get('body', '')
            })
        return comments


class CheckpointManager:
    """Manages scraping progress for resumability"""
    
    def __init__(self, checkpoint_file: str = 'checkpoint.json'):
        self.checkpoint_file = Path(checkpoint_file)
        self.state = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict:
        """Load existing checkpoint or create new one"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Corrupted checkpoint, starting fresh")
        
        return {'projects': {}}
    
    def save_checkpoint(self):
        """Save current state to disk"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        logger.debug(f"Checkpoint saved to {self.checkpoint_file}")
    
    def get_project_progress(self, project: str) -> int:
        """Get last processed index for a project"""
        return self.state['projects'].get(project, 0)
    
    def update_project_progress(self, project: str, start_at: int):
        """Update progress for a project"""
        self.state['projects'][project] = start_at
        self.save_checkpoint()


class DataTransformer:
    """Transforms raw Jira data into LLM training format"""
    
    @staticmethod
    def safe_get(data: Dict, *keys, default=''):
        """Safely navigate nested dictionaries"""
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, {})
            else:
                return default
        return data if data else default
    
    def transform_issue(self, issue_data: Dict, comments: List[Dict]) -> JiraIssue:
        """Transform raw API data into structured format"""
        fields = issue_data.get('fields', {})
        
        # Extract basic fields
        issue_id = issue_data.get('key', '')
        project = issue_id.split('-')[0] if issue_id else ''
        
        # Handle potentially missing fields
        title = self.safe_get(fields, 'summary', default='No Title')
        description = self.safe_get(fields, 'description', default='No Description')
        status = self.safe_get(fields, 'status', 'name', default='Unknown')
        priority = self.safe_get(fields, 'priority', 'name', default='Unknown')
        issue_type = self.safe_get(fields, 'issuetype', 'name', default='Unknown')
        reporter = self.safe_get(fields, 'reporter', 'displayName', default='Unknown')
        assignee = self.safe_get(fields, 'assignee', 'displayName', default=None)
        
        # Extract dates
        created_date = fields.get('created', '')
        updated_date = fields.get('updated', '')
        resolved_date = fields.get('resolutiondate')
        
        # Extract arrays
        labels = fields.get('labels', [])
        components = [c.get('name', '') for c in fields.get('components', [])]
        
        # Determine training task based on issue characteristics
        training_task = self._determine_training_task(issue_type, description, comments)
        
        return JiraIssue(
            issue_id=issue_id,
            project=project,
            title=title,
            description=description,
            status=status,
            priority=priority,
            issue_type=issue_type,
            reporter=reporter,
            assignee=assignee,
            created_date=created_date,
            updated_date=updated_date,
            resolved_date=resolved_date,
            labels=labels,
            components=components,
            comments=comments,
            training_task=training_task
        )
    
    def _determine_training_task(self, issue_type: str, description: str, 
                                  comments: List[Dict]) -> str:
        """Assign appropriate training task based on issue characteristics"""
        # Bugs with solutions are good for QnA
        if issue_type.lower() == 'bug' and len(comments) > 2:
            return 'question_answering'
        
        # Long descriptions are good for summarization
        if len(description) > 500:
            return 'summarization'
        
        # Issues with clear types are good for classification
        if issue_type.lower() in ['bug', 'improvement', 'new feature', 'task']:
            return 'classification'
        
        return 'general'


class JiraScraper:
    """Main scraper orchestrator"""
    
    def __init__(self, projects: List[str], output_dir: str = 'output'):
        self.projects = projects
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.api_client = JiraAPIClient(rate_limit_delay=1.0)
        self.checkpoint_manager = CheckpointManager()
        self.transformer = DataTransformer()
        
        self.stats = {
            'total_issues': 0,
            'total_comments': 0,
            'errors': 0
        }
    
    def scrape_project(self, project: str):
        """Scrape all issues for a given project"""
        logger.info(f"Starting scrape for project: {project}")
        output_file = self.output_dir / f"{project.lower()}_issues.jsonl"
        start_at = self.checkpoint_manager.get_project_progress(project)
        batch_size = 100        # fetch 100 issues at once
        max_workers = 5      # number of concurrent comment fetchers
        with open(output_file, 'a', encoding='utf-8') as f:
            while True:
                logger.info(f"Fetching {batch_size} {project} issues starting at {start_at}")

                # Fetch batch of issues
                response = self.api_client.search_issues(project, start_at)
                if not response:
                    logger.error(f"Failed to fetch issues for {project} at {start_at}")
                    self.stats['errors'] += 1
                    break

                issues = response.get('issues', [])
                total = response.get('total', 0)
                if not issues:
                    logger.info(f"No more issues for {project}")
                    break

                # Collect issue keys for batch comment fetching
                issue_keys = [issue.get('key', 'UNKNOWN') for issue in issues]
                comments_map = {}

                logger.info(f"Fetching comments for {len(issue_keys)} issues concurrently...")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_key = {
                        executor.submit(self.api_client.get_issue_comments, key): key
                        for key in issue_keys
                    }

                    for future in as_completed(future_to_key):
                        key = future_to_key[future]
                        try:
                            comments = future.result()
                            comments_map[key] = comments
                            self.stats['total_comments'] += len(comments)
                        except Exception as e:
                            logger.error(f"Failed to fetch comments for {key}: {e}")
                            comments_map[key] = []
                            self.stats['errors'] += 1

                # Process each issue after comments are fetched
                for issue_data in issues:
                    try:
                        issue_key = issue_data.get('key', 'UNKNOWN')
                        comments = comments_map.get(issue_key, [])

                        issue = self.transformer.transform_issue(issue_data, comments)
                        f.write(json.dumps(issue.to_dict(), ensure_ascii=False) + '\n')
                        self.stats['total_issues'] += 1

                    except Exception as e:
                        logger.error(f"Error processing issue {issue_key}: {e}")
                        self.stats['errors'] += 1

                # Update checkpoint
                start_at += len(issues)
                self.checkpoint_manager.update_project_progress(project, start_at)
                logger.info(f"Progress: {start_at}/{total} issues for {project}")

                # Stop when all issues are processed
                if start_at >= total:
                    break

        logger.info(f"✅ Completed scraping {project}")
        

    
    def run(self):
        """Execute scraping for all projects"""
        logger.info(f"Starting scraper for projects: {self.projects}")
        start_time = time.time()
        
        for project in self.projects:
            try:
                self.scrape_project(project)
            except Exception as e:
                logger.error(f"Fatal error scraping {project}: {e}")
                self.stats['errors'] += 1
        
        elapsed = time.time() - start_time
        
        # Print final statistics
        logger.info("=" * 50)
        logger.info("SCRAPING COMPLETE")
        logger.info(f"Total Issues: {self.stats['total_issues']}")
        logger.info(f"Total Comments: {self.stats['total_comments']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Time Elapsed: {elapsed:.2f}s")
        logger.info(f"Output Directory: {self.output_dir.absolute()}")
        logger.info("=" * 50)


def main():
    """Entry point"""
    # Import config if available, otherwise use defaults
    try:
        from config import PROJECTS, OUTPUT_DIR
        projects = PROJECTS
        output_dir = OUTPUT_DIR
    except ImportError:
        # Fallback to defaults if config.py not found
        projects = ['KAFKA', 'BEAM', 'HARMONY']
        output_dir = 'output'
        logger.warning("config.py not found, using default settings")
    
    logger.info(f"Starting scraper for projects: {projects}")
    
    scraper = JiraScraper(projects=projects, output_dir=output_dir)
    
    try:
        scraper.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Scraping interrupted by user")
        logger.info("Progress has been saved. Run again to resume.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
