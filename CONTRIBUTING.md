# Contributing to TAG

Thanks for your interest in contributing to TAG (Tigris Access Gateway)! This
document covers how to get set up, our conventions, and the contribution
process.

## Contributor License Agreement (CLA)

Before we can accept your contribution, you must sign the
[Contributor License Agreement](docs/CLA.md). This is a one-time process: the
first time you open a pull request, an automated check will comment with a link
to sign. Your PR cannot be merged until the CLA is signed.

The CLA lets Tigris Data include your contribution in TAG and its future
distributions while you retain ownership of your work. It does not change the
open-source license under which TAG is released ([Apache 2.0](LICENSE)).

## Development setup

Prerequisites, build, run, test, and code-quality commands are documented in the
[Contributing section of the README](README.md#contributing):

- Build: `make build`
- Test: `make test` (and the `test-*` targets for specific packages)
- Format & lint: `make fmt`, `make lint`, `make check`

All dependencies — including the Tigris [`ocache`](https://github.com/tigrisdata/ocache)
modules — are public, so a stock Go toolchain works with no extra configuration.

## Commit and pull request conventions

- **Conventional Commits.** PR titles and commit messages must use a
  [Conventional Commits](https://www.conventionalcommits.org/) prefix: `feat`,
  `fix`, `perf`, `docs`, `style`, `refactor`, `test`, `build`, `ci`, `chore`, or
  `revert` (e.g. `feat: add chunked transfer encoding support`). PR titles are
  enforced by CI.
- **Scope.** Keep PRs focused; unrelated changes belong in separate PRs.
- **Tests.** Add or update tests for behavior changes. `make check` must pass.
- **Docs.** Update the relevant docs under `docs/` and the README when behavior
  or configuration changes.

## Pull request process

1. Fork the repository and create a feature branch.
2. Make your change with tests and docs.
3. Ensure `make check` passes locally.
4. Open a pull request with a Conventional Commits title and a clear
   description of the change and its motivation.
5. Sign the CLA when prompted (first-time contributors).
6. Address review feedback. A maintainer will merge once approved and CI is
   green.

## Reporting bugs and requesting features

Please open a GitHub issue with a clear description, reproduction steps (for
bugs), and the version/commit you are running.

## Security issues

Please do not open public issues for security vulnerabilities. Instead, report
them privately to security@tigrisdata.com.
