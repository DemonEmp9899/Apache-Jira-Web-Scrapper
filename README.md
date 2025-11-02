🚀 Apache Jira Web Scraper

This project scrapes issue data (including metadata and comments) from Apache Jira projects like Kafka, Beam, and Harmony using the Jira REST API. The data is saved as JSON files for analysis.

🧩 Requirements

Before running the scraper, install all dependencies using:

pip install -r requirements_txt.txt

⚙️ Configuration

You can modify settings such as project name, batch size, or API URLs in config.py.

For Documentation read doc(1) file

▶️ Running the Scraper

To start scraping data, run:

python jira_scraper.py


This will:

Fetch issues from the configured Apache Jira project

Save results (issues and comments) to JSON files in the output/ directory

Log progress and errors in scraper.log

🧪 Testing the Scraper

After scraping, you can validate and test outputs using:

python test_scraper.py

and

python validate_output.py

📂 Output Files

checkpoint.json → Tracks progress for resuming scraping

scraper.log → Logs errors and progress

output/*.jsonl → Contains fetched issue data

sample_output.json → Example of output format

💡 Notes

If the script stops midway due to a network issue, simply rerun jira_scraper.py — it will resume from the last checkpoint.

Avoid pushing large output files (over 100MB) to GitHub
