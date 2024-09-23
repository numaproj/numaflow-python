# Release Guide

This document explains the release process for the Python SDK. You can find the most recent version under [Github Releases](https://github.com/numaproj/numaflow-python/releases).

### Before Release

1. Before releasing a new SDK version, make sure to update all references from the old version to the new one. For example,
the version in the root `pyproject.toml` should be updated (for [reference](https://github.com/numaproj/numaflow-python/commit/6a720e7c56121a45b94aa929c6b720312dd9340a))
   - The version in the root `pyproject.toml` can be updated by running `poetry version <version-bump-rule | PEP440-string>`. e.g., `poetry version 0.8.0`
   The version bump rules that can be provided and their corresponding effects can be found [here](https://python-poetry.org/docs/cli/#version),
   in the `Poetry` documentation. A version number can also be directly specified instead, but it must follow the [PEP 440](https://peps.python.org/pep-0440/) specification
2. If the version to be released has backwards incompatible changes, i.e. it does not support older versions of the Numaflow platform,
you must update the `MINIMUM_NUMAFLOW_VERSION` constant in the `pynumaflow/info/types.py` file to the minimum Numaflow version that is supported by your new SDK version.
3. After making these changes, create a PR and merge it.

### How to Release

This can be done via the Github UI. 
1. In the `Releases` section of the Python SDK repo, click `Draft a new release`
2. Create a tag that has the same name as the version that you specified in the root 
`pyproject.toml`, prefix it with a `'v'`, and select it
3. Make the title the same as the tag 
4. Click `Generate release notes` so that all the changes made since the last release are documented.
If there are any major features or breaking changes that you would like to highlight as part of the release, add those to the description as well
5. Select `Set as the latest release` or `Set as a pre-release`, depending on your situation
6. Finally, click `Publish release`, and your version tag will be the newest release on the repository
