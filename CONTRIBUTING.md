# Contributing

Thanks for contributing to `lazy_fred`.

## Local setup

```bash
poetry install --with dev
```

## Run checks

```bash
poetry run pytest -q
poetry run python -m ruff check .
```

If you want live FRED tests:

```bash
# PowerShell
$env:API_KEY="your_key_here"
poetry run pytest -q
```

## Style notes

- Keep beginner UX simple and explicit.
- Prefer safe defaults (`doctor`, `quick`, retries, backups).
- Avoid breaking terminal + Colab support.

## Pull request checklist

- [ ] Tests pass locally
- [ ] Lint passes locally
- [ ] README updated for user-facing changes
- [ ] Release notes impact considered (`RELEASING.md`)

