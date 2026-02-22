# GitHub Migration Guide (Private First)

This guide walks you through moving UTT into GitHub safely.

## 0) Before you start

You need:

- A GitHub account
- Git installed locally
- Your project folder initialized as a git repo

Check git is installed:

```bash
git --version
```

## 1) Prepare local project

From your project folder:

```bash
git status
```

If this is your first commit:

```bash
git add .
git commit -m "Initial project structure and documentation"
```

## 2) Create a private GitHub repository

1. Go to https://github.com/new
2. Repository name: `utt-unified-trading-terminal` (or your preferred name)
3. Visibility: **Private**
4. Do **not** initialize with README if your local repo already has files
5. Click **Create repository**

## 3) Connect local repo to GitHub

Copy your repo URL from GitHub, then run:

```bash
git remote add origin <YOUR_GITHUB_REPO_URL>
git branch -M main
git push -u origin main
```

Verify remote:

```bash
git remote -v
```

## 4) Protect private code and secrets

Before pushing more commits:

- Add `.env` and secrets to `.gitignore`
- Never hardcode API keys in source files
- Use GitHub Secrets for CI/CD and deployment
- Enable GitHub secret scanning and Dependabot alerts in repo settings

## 5) Set up branch protection (recommended)

In GitHub repo settings:

- Protect `main`
- Require pull requests before merge
- Require at least 1 approval
- Require status checks to pass
- Restrict force pushes

## 6) Add baseline project docs

Minimum files (included in this repo):

- `README.md`
- `SECURITY.md`
- `CONTRIBUTING.md`
- `docs/FAQ.md`
- `docs/HELP.md`
- `docs/BUILD_AND_USE.md`
- `docs/WHITEPAPER.md`

## 7) Publish strategy (private to public)

Use this sequence:

1. Keep repo private while architecture is unstable.
2. Invite 1-3 trusted reviewers.
3. Finalize install/build docs.
4. Remove secrets/history risks.
5. Create `v0.1.0` release notes.
6. Flip visibility to public only when ready.

## 8) Optional hardening checklist

- Enable 2FA on your GitHub account
- Use signed commits
- Add CI checks (lint/test/build)
- Add CODEOWNERS
- Add issue templates and PR template


## 9) Exact beginner push checklist (copy/paste)

Use these commands in order from your local project folder:

```bash
# 1) Confirm where you are
pwd

# 2) Confirm git sees your files
git status

# 3) Ensure secrets are ignored before commit
cat .gitignore

# 4) Create local env from template (safe)
cp .env.example .env

# 5) Stage and commit
git add .
git commit -m "chore: prepare project for private github"

# 6) Add your GitHub repo remote (replace URL)
git remote add origin <YOUR_GITHUB_REPO_URL>

# 7) Rename default branch to main
git branch -M main

# 8) Push to GitHub private repo
git push -u origin main

# 9) Verify remote tracking
git remote -v
git branch -vv
```

If `remote origin already exists`, run:

```bash
git remote set-url origin <YOUR_GITHUB_REPO_URL>
```

## 10) Post-push safety verification

After push, verify on GitHub:

1. Open repo and check no `.env` file is visible.
2. Confirm `.env.example` is present and contains placeholders only.
3. In **Settings → Security**, enable:
   - Secret scanning
   - Dependabot alerts
4. In **Settings → Branches**, add protection for `main`.
