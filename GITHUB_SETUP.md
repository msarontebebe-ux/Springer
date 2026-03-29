# How to Add This Project to GitHub

## Step 1: Create a GitHub Repository

1. Go to [GitHub](https://github.com) and sign in to your account
2. Click the **"+"** icon in the top right corner
3. Select **"New repository"**
4. Fill in the details:
   - **Repository name:** `springer-conference-scraper` (or your preferred name)
   - **Description:** "Scraper for Springer conference proceedings using CrossRef API with data lake architecture"
   - **Visibility:** Choose Public or Private
   - ⚠️ **DO NOT** check "Initialize this repository with a README" (we already have one)
   - ⚠️ **DO NOT** add .gitignore or license yet (we already have .gitignore)
5. Click **"Create repository"**

After creating, GitHub will show you a page with setup instructions. **Keep this page open!**

---

## Step 2: Initialize Git Locally (Run in PowerShell)

Open PowerShell in your project directory (`c:\Users\saron\project\`) and run:

```powershell
# Initialize Git repository
git init

# Check what files will be committed
git status

# You should see 11 files listed (data/ should NOT appear)
```

**Expected output:**
```
Untracked files:
  .gitignore
  config.json
  crossref_scraper.py
  DATA_LAKE.md
  filter_publications.py
  GIT_CHECKLIST.md
  main_crossref.py
  models.py
  README.md
  requirements.txt
  storage.py
```

---

## Step 3: Add and Commit Files

```powershell
# Add all files (data/ is automatically excluded by .gitignore)
git add .

# Verify what's staged
git status

# Create your first commit
git commit -m "Initial commit: Springer conference proceedings scraper with data lake architecture"
```

---

## Step 4: Connect to GitHub

Replace `YOUR_USERNAME` and `YOUR_REPO_NAME` with your actual GitHub username and repository name:

```powershell
# Add your GitHub repository as remote origin
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git

# Verify remote was added
git remote -v
```

**Example:**
```powershell
git remote add origin https://github.com/saron/springer-conference-scraper.git
```

---

## Step 5: Push to GitHub

```powershell
# Rename branch to 'main' (if needed)
git branch -M main

# Push your code to GitHub
git push -u origin main
```

You may be prompted to sign in to GitHub. Follow the authentication prompts.

---

## ✅ Verify Your Upload

1. Go to your GitHub repository page: `https://github.com/YOUR_USERNAME/YOUR_REPO_NAME`
2. You should see all 11 files
3. Verify the `data/` directory is **NOT** there (correctly excluded)
4. Check that README.md is displayed on the main page

---

## 🔐 Authentication Options

If you're prompted for authentication:

### Option 1: GitHub Desktop (Easiest)
1. Download [GitHub Desktop](https://desktop.github.com/)
2. Sign in with your GitHub account
3. Use the GUI to push your code

### Option 2: Personal Access Token (PAT)
1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token with `repo` scope
3. Use the token as your password when prompted

### Option 3: SSH Key
1. Generate SSH key: `ssh-keygen -t ed25519 -C "your_email@example.com"`
2. Add to GitHub: Settings → SSH and GPG keys → New SSH key
3. Change remote URL: `git remote set-url origin git@github.com:YOUR_USERNAME/YOUR_REPO_NAME.git`

---

## 📝 Future Updates

When you make changes to your project:

```powershell
# Check what changed
git status

# Add specific files or all changes
git add .

# Commit with a descriptive message
git commit -m "Add feature: description of changes"

# Push to GitHub
git push
```

---

## ⚠️ Important Notes

- ✅ Your `data/` directory (102 MB) is excluded - **good!**
- ✅ Only ~58 KB of code will be uploaded
- ✅ `.gitignore` prevents accidental uploads of large data files
- ⚠️ Never commit API keys or sensitive credentials
- 💡 Anyone cloning your repo can regenerate data using your scripts

---

## 🆘 Troubleshooting

### "fatal: not a git repository"
Run `git init` first

### "Permission denied (publickey)"
Set up SSH keys or use HTTPS with Personal Access Token

### "Updates were rejected because the remote contains work"
You may have initialized the repo with README on GitHub. Use:
```powershell
git pull origin main --allow-unrelated-histories
git push -u origin main
```

### Files you want ignored are being tracked
Make sure `.gitignore` is in the root directory and run:
```powershell
git rm -r --cached data/
git commit -m "Remove data directory from tracking"
git push
```
