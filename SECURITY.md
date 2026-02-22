# Security Policy

## Supported versions

Until formal releases exist, the latest `main` branch is the supported baseline.

## Reporting a vulnerability

Please **do not** open public issues for security vulnerabilities.

Instead:

1. Email: `security@your-domain.com` (replace with your actual address)
2. Subject: `[UTT SECURITY] <short description>`
3. Include:
   - Affected component
   - Reproduction steps
   - Potential impact
   - Suggested remediation (optional)

## Response targets

- Initial acknowledgment: within 72 hours
- Triage decision: within 7 days
- Patch timeline: based on severity and complexity

## Disclosure policy

We follow coordinated disclosure:

- Reporter and maintainer collaborate privately
- Fix is prepared and validated
- Public disclosure follows fix/release

## Secrets and key handling

- Never commit secrets to git
- Rotate exposed credentials immediately
- Use environment variables or secret managers

