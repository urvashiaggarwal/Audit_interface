# Property Data Audit & Scoring Platform

## Overview
This web application allows users to upload property project data, map and score it across multiple real estate sources, and download the processed results as a CSV. It is designed for auditing, matching, and scoring property data from sources like 99acres, MagicBricks, Housing, and SquareYards.

## Features
- **Upload XID CSV:** Upload a CSV file containing project XIDs to fetch and export detailed data from multiple sources.
- **Update Matching Score:** Trigger a full data mapping and scoring pipeline, updating the audit tables and running advanced scoring logic.
- **Get Output CSV:** Download the latest processed and scored results as a CSV file.
- **Loading Spinner & Error Handling:** User-friendly feedback for long-running operations and errors.

## Tech Stack
- Python (Flask, Pandas)
- MySQL
- HTML/CSS/JS (Jinja2 templates)
- Gemini AI (for advanced scoring, optional)

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the project root with the following variables:
```
MYSQL_HOST=your_mysql_host
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=your_database_name
GEMINI_API_KEY=your_gemini_api_key  # If using Gemini features
```

### 4. Database Setup
- Ensure your MySQL database is running and the required tables (`data_points`, `99acres_table`, `magic_bricks_table`, `housing_table`, `square_yards_table`, `competition_oprns_audit_data`, `bible_oprns_data`, etc.) are created.
- You may need to adjust table/column names in the code to match your schema.

### 5. Run the Application
```bash
python app.py
```
Visit [http://localhost:5000](http://localhost:5000) in your browser.

## Usage

### Upload XID CSV
- Click **"Upload and Fetch"** after selecting a CSV file with an `xid` column.
- The app fetches and exports data for those XIDs from all sources.

### Update Matching Score
- Click **"Update matching score"** to run the full data mapping and scoring pipeline.
- A loading spinner will appear; a message will display when complete or if there is an error.

### Get Output CSV
- Click **"Get Output CSV"** to download the latest processed results as a CSV file.

## File Structure
- `app.py` - Main Flask app and routes
- `tableinsertdata.py` - Data mapping/insertion logic
- `competition_operations.py` - Scoring and normalization logic
- `output_operations.py` - CSV export logic
- `templates/` - HTML templates
- `static/` - CSS and static assets
- `uploads/` - Uploaded files

## Contribution Guidelines
1. Fork this repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes and test thoroughly.
4. Submit a pull request with a clear description of your changes.

## License
[MIT License](LICENSE)

## Contact
For questions or support, open an issue or contact the maintainer. 