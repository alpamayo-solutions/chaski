# Contributing

Bug reports, questions and pull requests are welcome. This page says how to
work on chaski and what a change needs before it can be merged. Everyone taking
part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Set up

chaski depends on the data contracts in the Colca repository and is developed
against a checkout next to it:

```bash
git clone https://github.com/alpamayo-solutions/colca
git clone https://github.com/alpamayo-solutions/chaski
cd chaski
uv run --extra test pytest
```

The tests need no running node. The ones about `Node` use a fake colcad
process; set `COLCAD_BINARY` to a real binary (`make build` in the colca
checkout) to try the real one by hand.

CI also runs the linters, and so can you:

```bash
uv run --group lint ruff check . && uv run --group lint ruff format --check .
uv run --group lint bandit -q -c pyproject.toml -r src
uv run --extra dataops --group lint mypy src
```

The same checks, plus gitleaks and a check of the commit subject, run as
[pre-commit](https://pre-commit.com) hooks once you install them:

```bash
uv tool install pre-commit
pre-commit install
```

## Pull requests

- Keep a pull request to one change, and add or adjust a test that fails
  without it.
- If the change needs something from a node, change Colca first; chaski follows
  what a node does, it does not work around it.
- Write the commit subject as `type(scope): what changes`, for example
  `fix(dataops): trim the buffer by the horizons resolved now`. Types are `feat`,
  `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci` and `chore`.
- Explain in the pull request why the change is needed.

## Keep working notes out of the repository

Plans, design drafts, task lists and instruction files for coding assistants
(`CLAUDE.md`, `AGENTS.md`, `.cursor/`, `docs/plans/` and the like) do not
belong in commits. CI rejects them; `scripts/check-no-working-notes.sh` runs
the same check locally.

## Releases

Maintainers release by pushing a tag `vX.Y.Z`, or `vX.Y.Z-rc.N` for a release
candidate, on `main`. Once lint and tests have passed, CI creates a GitHub
release with the wheel, the sdist, an SBOM, checksums, and notes built from
the commit subjects.

## Contributor License Agreement

Before your first pull request can be merged you sign the
[Contributor License Agreement](CLA.md). A bot asks for it on the pull
request. You keep the copyright in your work.

chaski is licensed under the Functional Source License with an Apache 2.0
future grant (see [LICENSE.md](LICENSE.md)).
