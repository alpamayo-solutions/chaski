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

[release-please](https://github.com/googleapis/release-please) keeps a release
pull request open that proposes the next version and the changelog, both taken
from the commit subjects: `fix` bumps the patch version, `feat` and breaking
changes bump the minor version. The major version never changes on its own;
1.0 and later are set by hand. Merging that pull request tags the release, and
CI then attaches the wheel, the sdist, an SBOM and checksums to the GitHub
release and uploads the wheel and sdist to PyPI.

## Contributor License Agreement

Unless you belong to the organisation that owns this repository, tick the box
"I agree to the Contributor License Agreement" in each pull request's
description; a check blocks the merge until it is ticked. The agreement is in
[CLA.md](CLA.md). For larger contributions, or when you contribute for your
employer, we may also ask for a signed copy by email. You keep the copyright in
your work.

chaski is licensed under the Functional Source License with an Apache 2.0
future grant (see [LICENSE.md](LICENSE.md)).
