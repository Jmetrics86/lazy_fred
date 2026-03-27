# Releasing lazy_fred

## 1) Bump version

Update `version` in `pyproject.toml`:

```toml
version = "0.1.xx"
```

## 2) Run local checks

```bash
poetry install --with dev
poetry run pytest -q
poetry run python -m ruff check .
poetry build
```

## 3) Commit and tag

```bash
git add .
git commit -m "release: v0.1.xx"
git tag v0.1.xx
git push origin HEAD
git push origin v0.1.xx
```

## 4) Publish

Preferred:
- Publish GitHub Release from tag `v0.1.xx`
- `publish-pypi.yml` runs verify -> publish (Trusted Publishing)

Manual fallback:
- Actions -> `Publish to PyPI`
- `workflow_dispatch`
- choose `publish_method=poetry` (requires `PYPI_API_TOKEN` secret)

## 5) Verify release

- Check package: https://pypi.org/project/lazy_fred/
- In clean environment:

```bash
python -m pip install -U lazy_fred
lazy-fred doctor
```

