# Help and Troubleshooting

## Common setup issues

### Git push fails with authentication error

- Confirm you're logged into GitHub
- Use a Personal Access Token or SSH key
- Re-check remote URL with `git remote -v`

### Remote already exists

Use:

```bash
git remote set-url origin <YOUR_GITHUB_REPO_URL>
```

### Accidentally committed secrets

1. Revoke/rotate secret immediately
2. Remove secret from files
3. Rewrite git history (if required)
4. Force push cleaned history

### Build errors after cloning

- Verify runtime versions
- Install dependencies again
- Re-run documented build steps

## Getting support

When requesting help, include:

- OS and version
- Exact command run
- Error output
- Recent changes made

