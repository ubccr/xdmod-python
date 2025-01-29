# xdmod-data Changelog

## v1.x.y (development branch)

- Prepare for v1.1.0 development ([\#44](https://github.com/ubccr/xdmod-data/pull/44)).
- Implement 100% test coverage ([\#45](https://github.com/ubccr/xdmod-data/pull/45)).
- Update Flake8 rules ([\#46](https://github.com/ubccr/xdmod-data/pull/46)).
- Change to using CircleCI instead of GitHub Actions for continuous integration
  ([\#50](https://github.com/ubccr/xdmod-data/pull/50),
  [\#53](https://github.com/ubccr/xdmod-data/pull/53),
  [\#56](https://github.com/ubccr/xdmod-data/pull/56),
  [\#58](https://github.com/ubccr/xdmod-data/pull/58)).

## v1.0.2 (2024-10-31)

This release fixes an `IOPub` error that can occur when calling
`get_raw_data()` with `show_progress=True`.

It is compatible with Open XDMoD versions 11.0.x and 10.5.x.

- Document Open XDMoD compatibility in changelog ([\#31](https://github.com/ubccr/xdmod-data/pull/31)).
- Fix IOPub error when showing progress with `get_raw_data()` ([\#40](https://github.com/ubccr/xdmod-data/pull/40)).

## v1.0.1 (2024-09-27)

This release has bug fixes, performance improvements, and updates for
compatibility, tests, and documentation.

It is compatible with Open XDMoD versions 11.0.x and 10.5.x.

- Add borders to README images ([\#12](https://github.com/ubccr/xdmod-data/pull/12)).
- Create `PULL_REQUEST_TEMPLATE.md` ([\#13](https://github.com/ubccr/xdmod-data/pull/13)).
- Add types of changes to pull request template ([\#15](https://github.com/ubccr/xdmod-data/pull/15)).
- Add Changelog ([\#17](https://github.com/ubccr/xdmod-data/pull/17)).
- Update tests and testing instructions ([\#14](https://github.com/ubccr/xdmod-data/pull/14)).
- Remove limit on number of results returned from `get_filter_values()` ([\#21](https://github.com/ubccr/xdmod-data/pull/21)).
- Add citation for the DAF paper ([\#23](https://github.com/ubccr/xdmod-data/pull/23)).
- Add a "Feedback / Feature Requests" section to the README ([\#22](https://github.com/ubccr/xdmod-data/pull/22)).
- Improve performance of validation of filters and raw fields ([\#18](https://github.com/ubccr/xdmod-data/pull/18)).
- Fix bug with trailing slashes in `xdmod_host` ([\#24](https://github.com/ubccr/xdmod-data/pull/24)).
- Use streaming for raw data requests ([\#19](https://github.com/ubccr/xdmod-data/pull/19)).
- Specify minimum version numbers for dependencies ([\#25](https://github.com/ubccr/xdmod-data/pull/25)).

## v1.0.0 (2023-07-21)

Initial release.

Compatible with Open XDMoD version 10.5.x.
