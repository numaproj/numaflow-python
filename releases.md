# Release Guide

This document explains the release process for the Python SDK. You can find the most recent version under [Github Releases](https://github.com/numaproj/numaflow-python/releases).


### Before Release

Before releasing a new SDK version, make sure to update all references from the old version to the new one. For example,
the version in the root `pyproject.toml` should be updated (for [reference](https://github.com/numaproj/numaflow-python/commit/6a720e7c56121a45b94aa929c6b720312dd9340a)), 
which can be done by running `poetry version <version-bump-rule | PEP440-string>`. The version bump rules that can be provided and their corresponding effects can be found [here](https://python-poetry.org/docs/cli/#version),
in the poetry documentation. A version number can also be directly specified instead, but it must follow the [PEP 440](https://peps.python.org/pep-0440/) specification.
After making these changes, create a PR. Once merged, it will trigger the Docker Publish workflow, and should be included in the release.
As a result, the correct SDK version will always be printed in the server information logs, and
the example images will always be using the latest changes (due to referencing the local SDK tarball that is built).

If the version to be released version has backwards incompatible changes, i.e. it does not support older versions of the Numaflow platform,
you must update the `MINIMUM_NUMAFLOW_VERSION` constant in the `pynumaflow/info/types.py` file to the minimum Numaflow version that is supported by your new SDK version.
Ensure that this change is merged and included in the release. 

### How to Release

This can be done via the Github UI. In the `Releases` section of the Python SDK repo, click `Draft a new release`. Create a tag that has the same name as the version that you specified in the root 
`pyproject.toml` prefixed with a 'v', and select it. Make the title the same as the tag. Click `Generate release notes` so that 
all the changes made since the last release are documented. If there are any major features or breaking changes that you would like to highlight as part of the release, 
add those to the description as well. Then set the release as either pre-release or latest, depending on your situation. Finally, click `Publish release`, and your version tag will be the newest release on the repository.
