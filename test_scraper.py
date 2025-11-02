"""
Unit tests for Jira Scraper
Run with: python test_scraper.py
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import tempfile
from pathlib import Path
from jira_scraper import (
    JiraAPIClient, CheckpointManager, DataTransformer, 
    JiraIssue, JiraScraper
)


class TestJiraAPIClient(unittest.TestCase):
    """Test API client functionality"""
    
    def setUp(self):
        self.client = JiraAPIClient(rate_limit_delay=0.1)
    
    @patch('jira_scraper.requests.Session.get')
    def test_successful_request(self, mock_get):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'key': 'value'}
        mock_get.return_value = mock_response
        
        result = self.client._make_request('http://test.com')
        self.assertEqual(result, {'key': 'value'})
    
    @patch('jira_scraper.requests.Session.get')
    @patch('time.sleep')
    def test_rate_limit_handling(self, mock_sleep, mock_get):
        """Test HTTP 429 rate limit handling"""
        # First call returns 429, second succeeds
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {'Retry-After': '1'}
        
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {'success': True}
        
        mock_get.side_effect = [mock_response_429, mock_response_200]
        
        result = self.client._make_request('http://test.com')
        self.assertEqual(result, {'success': True})
        mock_sleep.assert_called()
    
    @patch('jira_scraper.requests.Session.get')
    @patch('time.sleep')
    def test_retry_on_server_error(self, mock_sleep, mock_get):
        """Test retry logic on 5xx errors"""
        # First two calls fail, third succeeds
        mock_response_500 = Mock()
        mock_response_500.status_code = 500
        
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {'success': True}
        
        mock_get.side_effect = [
            mock_response_500, 
            mock_response_500, 
            mock_response_200
        ]
        
        result = self.client._make_request('http://test.com', max_retries=3)
        self.assertEqual(result, {'success': True})
        self.assertEqual(mock_sleep.call_count, 2)  # 2 retries
    
    @patch('jira_scraper.requests.Session.get')
    def test_max_retries_exhausted(self, mock_get):
        """Test behavior when max retries reached"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        result = self.client._make_request('http://test.com', max_retries=2)
        self.assertIsNone(result)


class TestCheckpointManager(unittest.TestCase):
    """Test checkpoint management"""
    
    def setUp(self):
        # Create temporary file for testing
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.temp_file.close()
        self.checkpoint_file = self.temp_file.name
    
    def tearDown(self):
        # Clean up temp file
        Path(self.checkpoint_file).unlink(missing_ok=True)
    
    def test_new_checkpoint(self):
        """Test creating new checkpoint"""
        manager = CheckpointManager(self.checkpoint_file)
        self.assertEqual(manager.state, {'projects': {}})
    
    def test_save_and_load_checkpoint(self):
        """Test saving and loading checkpoint"""
        manager = CheckpointManager(self.checkpoint_file)
        manager.update_project_progress('KAFKA', 100)
        
        # Create new manager to test loading
        manager2 = CheckpointManager(self.checkpoint_file)
        self.assertEqual(manager2.get_project_progress('KAFKA'), 100)
    
    def test_multiple_projects(self):
        """Test tracking multiple projects"""
        manager = CheckpointManager(self.checkpoint_file)
        manager.update_project_progress('KAFKA', 100)
        manager.update_project_progress('SPARK', 200)
        
        self.assertEqual(manager.get_project_progress('KAFKA'), 100)
        self.assertEqual(manager.get_project_progress('SPARK'), 200)
        self.assertEqual(manager.get_project_progress('UNKNOWN'), 0)


class TestDataTransformer(unittest.TestCase):
    """Test data transformation"""
    
    def setUp(self):
        self.transformer = DataTransformer()
    
    def test_safe_get(self):
        """Test safe dictionary navigation"""
        data = {'a': {'b': {'c': 'value'}}}
        
        result = self.transformer.safe_get(data, 'a', 'b', 'c')
        self.assertEqual(result, 'value')
        
        # Test with missing key
        result = self.transformer.safe_get(data, 'x', 'y', 'z', default='default')
        self.assertEqual(result, 'default')
    
    def test_transform_complete_issue(self):
        """Test transforming issue with all fields"""
        issue_data = {
            'key': 'KAFKA-12345',
            'fields': {
                'summary': 'Test Bug',
                'description': 'Test description',
                'status': {'name': 'Open'},
                'priority': {'name': 'Major'},
                'issuetype': {'name': 'Bug'},
                'reporter': {'displayName': 'John Doe'},
                'assignee': {'displayName': 'Jane Smith'},
                'created': '2024-01-15T10:00:00.000+0000',
                'updated': '2024-01-16T10:00:00.000+0000',
                'resolutiondate': None,
                'labels': ['test', 'bug'],
                'components': [{'name': 'core'}, {'name': 'consumer'}]
            }
        }
        
        comments = [
            {'author': 'User1', 'created': '2024-01-15', 'body': 'Comment 1'}
        ]
        
        issue = self.transformer.transform_issue(issue_data, comments)
        
        self.assertEqual(issue.issue_id, 'KAFKA-12345')
        self.assertEqual(issue.project, 'KAFKA')
        self.assertEqual(issue.title, 'Test Bug')
        self.assertEqual(issue.status, 'Open')
        self.assertEqual(issue.priority, 'Major')
        self.assertEqual(len(issue.labels), 2)
        self.assertEqual(len(issue.components), 2)
    
    def test_transform_missing_fields(self):
        """Test transforming issue with missing fields"""
        issue_data = {
            'key': 'TEST-1',
            'fields': {}
        }
        
        issue = self.transformer.transform_issue(issue_data, [])
        
        self.assertEqual(issue.title, 'No Title')
        self.assertEqual(issue.description, 'No Description')
        self.assertEqual(issue.status, 'Unknown')
        self.assertIsNone(issue.assignee)
    
    def test_training_task_assignment(self):
        """Test training task assignment logic"""
        # Bug with multiple comments -> QnA
        task = self.transformer._determine_training_task(
            'Bug', 'Short desc', [{'body': 'c1'}, {'body': 'c2'}, {'body': 'c3'}]
        )
        self.assertEqual(task, 'question_answering')
        
        # Long description -> Summarization
        task = self.transformer._determine_training_task(
            'Task', 'x' * 600, []
        )
        self.assertEqual(task, 'summarization')
        
        # Clear type -> Classification
        task = self.transformer._determine_training_task(
            'New Feature', 'Short', []
        )
        self.assertEqual(task, 'classification')


class TestJiraScraper(unittest.TestCase):
    """Test main scraper orchestration"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.scraper = JiraScraper(['TEST'], output_dir=self.temp_dir)
    
    def tearDown(self):
        # Clean up temp directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch.object(JiraAPIClient, 'search_issues')
    @patch.object(JiraAPIClient, 'get_issue_comments')
    def test_scrape_single_batch(self, mock_comments, mock_search):
        """Test scraping a single batch of issues"""
        # Mock API responses
        mock_search.return_value = {
            'total': 2,
            'issues': [
                {
                    'key': 'TEST-1',
                    'fields': {
                        'summary': 'Issue 1',
                        'description': 'Desc 1',
                        'status': {'name': 'Open'},
                        'priority': {'name': 'Major'},
                        'issuetype': {'name': 'Bug'},
                        'reporter': {'displayName': 'User1'},
                        'assignee': {'displayName': 'User2'},
                        'created': '2024-01-01',
                        'updated': '2024-01-02',
                        'resolutiondate': None,
                        'labels': [],
                        'components': []
                    }
                },
                {
                    'key': 'TEST-2',
                    'fields': {
                        'summary': 'Issue 2',
                        'description': 'Desc 2',
                        'status': {'name': 'Closed'},
                        'priority': {'name': 'Minor'},
                        'issuetype': {'name': 'Task'},
                        'reporter': {'displayName': 'User3'},
                        'assignee': None,
                        'created': '2024-01-03',
                        'updated': '2024-01-04',
                        'resolutiondate': '2024-01-04',
                        'labels': ['test'],
                        'components': []
                    }
                }
            ]
        }
        
        mock_comments.return_value = []
        
        # Run scraper
        self.scraper.scrape_project('TEST')
        
        # Verify output file exists
        output_file = Path(self.temp_dir) / 'test_issues.jsonl'
        self.assertTrue(output_file.exists())
        
        # Verify content
        with open(output_file, 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 2)
            
            issue1 = json.loads(lines[0])
            self.assertEqual(issue1['issue_id'], 'TEST-1')
            self.assertEqual(issue1['title'], 'Issue 1')
            
            issue2 = json.loads(lines[1])
            self.assertEqual(issue2['issue_id'], 'TEST-2')
            self.assertEqual(issue2['title'], 'Issue 2')


def run_tests():
    """Run all tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestJiraAPIClient))
    suite.addTests(loader.loadTestsFromTestCase(TestCheckpointManager))
    suite.addTests(loader.loadTestsFromTestCase(TestDataTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestJiraScraper))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*70)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
