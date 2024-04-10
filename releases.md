# Release Guide

This document explains the release process for the Python SDK. You can find the most recent version under [Github Releases](https://github.com/numaproj/numaflow-python/releases).


### Before Release

Before releasing a new SDK version, make sure to update all references from the old version to the new one. For example,
the version in the root `pyproject.toml` should be updated (for [reference](https://github.com/numaproj/numaflow-python/commit/6a720e7c56121a45b94aa929c6b720312dd9340a)), 
which can be done by running `poetry version <new-version>`.
After making these changes, create a PR. Once merged, it will trigger the Docker Publish workflow, and should be included in the release.
As a result, the correct SDK version will always be printed in the server information logs, and
the example images will always be using the latest changes (due to referencing the local SDK tarball that is built).

### How to Release

This can be done via the Github UI. In the `Releases` section of the Python SDK repo, click `Draft a new release`. Create an appropriate tag for the new version and select it. Make 
the title the same as the tag. Click `Generate release notes` so that all the changes made since the last release are documented. If there are any major features or breaking
changes that you would like to highlight as part of the release, add those to the description as well. Then set the release as either pre-release or latest, depending
on your situation. Finally, click `Publish release`, and your version tag will be the newest release on the repository (assuming that you chose `Set as the latest release`).
