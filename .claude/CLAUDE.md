# Conventions for AI assistants

## Pull request titles and releases

The pull request title decides the next version of this module, so write it as
`type(optional-scope): description` with one of these types:

| Title starts with | New version |
|---|---|
| `feat:` | minor step (v0.4.2 → v0.5.0) |
| `fix:`, `perf:`, `revert:` | patch step (v0.4.2 → v0.4.3) |
| `docs:`, `chore:`, `test:`, `ci:`, `build:`, `refactor:`, `style:` | no new version |
| any type with `!`, e.g. `feat!:` | breaking change; while on v0.x a minor step |

- Choose the type by what the change means for users of this module, not by
  the files it touches.
- Maintainers can override the outcome with a label `release:none`,
  `release:patch`, `release:minor` or `release:major`.
- Never create, move or delete version tags (`vX.Y.Z`) yourself. Tags are
  permanent: the Go module proxy keeps every published tag. Versions are tagged
  after the merge, by maintainers and later by the release workflow.
