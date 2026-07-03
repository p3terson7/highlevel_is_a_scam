# AGENTS.md

This is a production Python SaaS application migrating from server-rendered/static HTML to React.

## Rules

- Preserve existing behavior unless the task explicitly says otherwise.
- Do not perform big-bang rewrites.
- Prefer small, reviewable patches.
- Keep business logic in Python.
- Do not duplicate permissions, billing, workflow, or tenant logic in React.
- Preserve authentication, sessions, CSRF, redirects, and existing URLs.
- Do not expose secrets in frontend code.
- Do not remove old templates/static files until the replacement is tested and explicitly approved.
- Do not introduce major dependencies without explaining why.
- Keep changes scoped to the requested phase.

## Before coding

- Read README, project docs, package files, Python dependency files, CI config, Docker files, and deployment scripts.
- Identify the Python framework and current test commands.
- Run baseline tests if feasible.
- Document existing failures before making changes.

## Frontend conventions

- Prefer React + TypeScript unless project constraints say otherwise.
- Keep API calls in an API/client layer.
- Keep presentational components free of data-fetching logic when practical.
- Use accessible markup.
- Include loading, error, empty, and permission states.
- Avoid unnecessary global state.
- Keep feature code organized by domain.

## Backend conventions

- Keep routes/controllers thin.
- Reuse existing services/models/forms.
- Keep permission checks server-side.
- Add API tests for new endpoints.
- Use consistent JSON response and error shapes.
- Preserve existing logging and error handling.

## Required validation

Run the most relevant available commands, such as:
- Python tests.
- Frontend build.
- Frontend tests.
- Linters/type checks.
- Any project-specific CI commands.

If a command cannot run, explain why.