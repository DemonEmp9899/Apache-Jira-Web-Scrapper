"""
Output Validator - Validates JSONL output files
Usage: python validate_output.py
"""

import json
import sys
from pathlib import Path
from collections import Counter


def validate_jsonl_file(filepath: Path) -> dict:
    """Validate a JSONL file and return statistics"""
    
    stats = {
        'total_lines': 0,
        'valid_lines': 0,
        'invalid_lines': 0,
        'required_fields': [
            'issue_id', 'project', 'title', 'description',
            'status', 'priority', 'issue_type', 'reporter',
            'created_date', 'training_task'
        ],
        'missing_fields': Counter(),
        'training_tasks': Counter(),
        'projects': Counter(),
        'errors': []
    }
    
    print(f"\nValidating: {filepath}")
    print("=" * 60)
    
    if not filepath.exists():
        print(f"❌ File not found: {filepath}")
        return stats
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            stats['total_lines'] += 1
            
            try:
                # Parse JSON
                data = json.loads(line.strip())
                
                # Check required fields
                missing = []
                for field in stats['required_fields']:
                    if field not in data:
                        missing.append(field)
                        stats['missing_fields'][field] += 1
                
                if missing:
                    stats['invalid_lines'] += 1
                    error_msg = f"Line {line_num}: Missing fields {missing}"
                    stats['errors'].append(error_msg)
                else:
                    stats['valid_lines'] += 1
                    
                    # Collect statistics
                    stats['training_tasks'][data.get('training_task', 'unknown')] += 1
                    stats['projects'][data.get('project', 'unknown')] += 1
                
            except json.JSONDecodeError as e:
                stats['invalid_lines'] += 1
                error_msg = f"Line {line_num}: Invalid JSON - {str(e)}"
                stats['errors'].append(error_msg)
                
            except Exception as e:
                stats['invalid_lines'] += 1
                error_msg = f"Line {line_num}: Unexpected error - {str(e)}"
                stats['errors'].append(error_msg)
    
    return stats


def print_statistics(stats: dict):
    """Print validation statistics"""
    
    print(f"\n📊 Statistics:")
    print(f"  Total lines: {stats['total_lines']}")
    print(f"  ✓ Valid: {stats['valid_lines']}")
    print(f"  ✗ Invalid: {stats['invalid_lines']}")
    
    if stats['valid_lines'] > 0:
        success_rate = (stats['valid_lines'] / stats['total_lines']) * 100
        print(f"  Success rate: {success_rate:.2f}%")
    
    if stats['training_tasks']:
        print(f"\n📚 Training Tasks Distribution:")
        for task, count in stats['training_tasks'].most_common():
            percentage = (count / stats['valid_lines']) * 100
            print(f"  {task}: {count} ({percentage:.1f}%)")
    
    if stats['projects']:
        print(f"\n📁 Projects:")
        for project, count in stats['projects'].most_common():
            print(f"  {project}: {count} issues")
    
    if stats['missing_fields']:
        print(f"\n⚠️  Most Common Missing Fields:")
        for field, count in stats['missing_fields'].most_common(5):
            print(f"  {field}: {count} occurrences")
    
    if stats['errors']:
        print(f"\n❌ Errors (showing first 10):")
        for error in stats['errors'][:10]:
            print(f"  {error}")
        
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")


def validate_all_outputs(output_dir: str = 'output'):
    """Validate all JSONL files in output directory"""
    
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Output directory not found: {output_path}")
        return False
    
    jsonl_files = list(output_path.glob('*.jsonl'))
    
    if not jsonl_files:
        print(f"❌ No JSONL files found in {output_path}")
        return False
    
    print(f"\n🔍 Found {len(jsonl_files)} JSONL file(s)")
    
    all_valid = True
    total_stats = {
        'total_issues': 0,
        'total_valid': 0,
        'total_invalid': 0
    }
    
    for jsonl_file in jsonl_files:
        stats = validate_jsonl_file(jsonl_file)
        print_statistics(stats)
        
        total_stats['total_issues'] += stats['total_lines']
        total_stats['total_valid'] += stats['valid_lines']
        total_stats['total_invalid'] += stats['invalid_lines']
        
        if stats['invalid_lines'] > 0:
            all_valid = False
        
        print("\n" + "=" * 60)
    
    # Print overall summary
    print(f"\n{'='*60}")
    print("📋 OVERALL SUMMARY")
    print(f"{'='*60}")
    print(f"Total issues processed: {total_stats['total_issues']}")
    print(f"✓ Valid issues: {total_stats['total_valid']}")
    print(f"✗ Invalid issues: {total_stats['total_invalid']}")
    
    if total_stats['total_issues'] > 0:
        success_rate = (total_stats['total_valid'] / total_stats['total_issues']) * 100
        print(f"Overall success rate: {success_rate:.2f}%")
    
    if all_valid:
        print(f"\n✅ All files are valid!")
        return True
    else:
        print(f"\n⚠️  Some files have validation errors")
        return False


def main():
    """Main entry point"""
    print("="*60)
    print("JIRA SCRAPER OUTPUT VALIDATOR")
    print("="*60)
    
    success = validate_all_outputs()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
