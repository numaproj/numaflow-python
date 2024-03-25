# Developer Guide

This document explains the development process for the Numaflow Python SDK.

### Testing

1. Install [Poetry](https://python-poetry.org/docs/) before starting your test. Make sure you have the correct Python version
2. Push your code change to your remote branch
3. Update the `pynumaflow` dependency in the `pyproject.toml` file with your (forked) repo url and branch name. For example, `pynumaflow = {git = "https://github.com/<github-username>/numaflow-python", rev = <branch-name>}`
4. Run `poetry update -vv` from this `developer_guide` folder. You should get a `poetry.lock` file
5. Update your `example.py` as/if needed
6. Run `make image` to build your image
7. Now that you have the image with your customized example and code change, you can test it in a Numaflow pipeline. An example pipeline `pipeline-numaflow.yaml` is also provided in this `developer_guide` folder.
Please check [Numaflow](https://numaflow.numaproj.io/) for more details

Each example directory has a Makefile which can be used to build, tag, and push images.
If you want to build and tag the image and then immediately push it to quay.io, use the `image-push` target.
If you want to build and tag a local image without pushing, use the `image` target.

If you want to build and push all the example images at once, you can run:
```shell
./.hack/update_examples.sh -bp -t <tag>
```
The default tag is `stable`, but it is recommended you specify your own for testing purposes, as the Github Actions CI uses the `stable` tag.
This consistent tag name is used so that the tags in the [E2E test pipelines](https://github.com/numaproj/numaflow/tree/main/test) do not need to be
updated each time an SDK change is made.

You can alternatively build and push a specific example image by running the following:
```shell
./.hack/update_examples.sh -bpe <path-to-dockerfile> -t <tag>
```
This is essentially equivalent to running `make image-push TAG=<tag>` in the example directory itself.

Note: before running the script, ensure that through the CLI, you are logged into quay.io.

### Deploying

After confirming that your changes pass local testing:
1. Clean up testing artifacts
2. Revert the `pyproject.toml` file to its previous state, i.e. before you updated it with your forked repo and branch
3. Create a PR for your changes

Once the PR has been merged it is important that the `pynumaflow` dependency of the example images use your merged PR's commit SHA
as reference. Thus, before you delete/leave your branch, run:
```shell
./.hack/update_examples.sh -us <commit-sha>
./.hack/update_examples.sh -bp
```
The above will update the `pynumaflow` dependencies to point to the specified commit SHA, and build, tag, and push all example images.
Since we do not want to flood the commit history with dependency updates, it is not necessary
to create a second PR with these changes. It is not necessary, as the `pynumaflow` dependency of the images will be
`pynumaflow = {git = "https://github.com/numaproj/numaflow-python.git", rev = "<latest-sha>"}`, while the repo itself will show
`pynumaflow = "~<latest-version>"`.

### After Release

Once a new release has been made, and its corresponding version tag exists on the remote repo, we want to update the
`pyproject.toml` files to reflect this new version:
```shell
./.hack/update_examples.sh -uv <version>
  ```
After running the above, create a PR for the changes that the script made.

Once your changes have been merged, similar to the deploying section above, before deleting/leaving your branch, update
the example images to use the merged commit SHA:
```shell
./.hack/update_examples.sh -us <commit-sha>
./.hack/update_examples.sh -bp
```
As a result, the correct SDK version will always be printed in the server information logs, and
the example images will always be using the latest commit SHA.
