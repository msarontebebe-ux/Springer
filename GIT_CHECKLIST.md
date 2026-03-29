# Git Repository Checklist

## ✅ Files to Include in Git

### Core Source Code (Required)
- ✅ `main_crossref.py` - Main script to run the scraper
- ✅ `crossref_scraper.py` - CrossRef API client
- ✅ `storage.py` - Data lake storage handler
- ✅ `models.py` - Data models (Publication, Author)
- ✅ `filter_publications.py` - Utility to filter data by year

### Configuration (Required)
- ✅ `config.json` - Series configuration with ISSNs
- ✅ `requirements.txt` - Python dependencies

### Documentation (Required)
- ✅ `README.md` - Project overview and usage
- ✅ `DATA_LAKE.md` - Data architecture documentation
- ✅ `.gitignore` - Files to exclude from Git

### Optional/Consider
- ⚠️ `filter_data.py` - Appears to be duplicate of `filter_publications.py`
  - Suggest deleting if redundant

---

## ❌ Files to EXCLUDE (Already in .gitignore)

### Data Directory (102 MB!)
- ❌ `data/` - All data files
  - Contains 46K+ publications
  - Too large for Git
  - Can be regenerated using the scripts

### Python Generated Files
- ❌ `__pycache__/` - Python cache
- ❌ `*.pyc` - Compiled Python files
- ❌ `.venv/` - Virtual environment

---

## 🚀 Ready for Git Commands

### First Time Setup
```bash
# Initialize Git repository
git init

# Add all essential files (data/ is already ignored)
git add .

# Create first commit
git commit -m "Initial commit: Springer conference proceedings scraper"

# Add remote repository
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git

# Push to GitHub
git push -u origin main
```

### Update Existing Repository
```bash
# Check what's changed
git status

# Add changes
git add .

# Commit with message
git commit -m "Update: [describe your changes]"

# Push to remote
git push
```

---

## 📊 Repository Size

**With data files:** ~102 MB ❌ Too large!
**Without data files:** ~50 KB ✅ Perfect for Git!

---

## 💡 Regenerating Data

Anyone who clones your repository can regenerate the data:

```bash
# Install dependencies
pip install -r requirements.txt

# Scrape recent data (2025-2026)
python main_crossref.py --year-start 2025 --max-results 5000

# Filter for live display
python filter_publications.py --output-dir data/live --year-start 2025
```

---

## 🔒 Before Pushing

**Check these:**
1. ✅ No sensitive data in `config.json` (API keys, passwords)
2. ✅ `.gitignore` is correctly configured
3. ✅ `data/` directory is not being tracked
4. ✅ Documentation is up-to-date

**Verify what will be pushed:**
```bash
git status
git diff --cached
```

---

## 📝 Suggested Commit Messages

- `Initial commit: Springer conference proceedings scraper`
- `Add data lake architecture with live/archive separation`
- `Add filter utility for year-based data separation`
- `Update documentation for data organization`
- `Fix: [describe bug fix]`
- `Feature: [describe new feature]`
