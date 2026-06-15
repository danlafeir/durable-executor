# Agent guide

## Git workflow

Commit directly to `main` and push. Do **not** create feature branches, and do **not** wait for a
"Fast-forward main and push" gate — that earlier convention is retired (maintainer request, 2026-06-14).

- Keep each commit small and self-contained (one logical change).
- Run `./gradlew test` and keep `main` green before pushing — there is no feature-branch buffer now.
- Commits are gpg-signed; end commit messages with the `Co-Authored-By: Claude ...` trailer.
