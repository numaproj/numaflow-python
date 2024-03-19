# Developer Guide

This example is for numaflow-python contributors/developers. The example includes how to use your branch to build the UDF image to test your code change before submitting a PR.

### Testing

1. Install [Poetry](https://python-poetry.org/docs/) before starting your test. Make sure you have the correct Python version.
2. Push your code change to your branch.
3. Update the `pynumaflow` dependency in `pyproject.toml` file with your (forked) repo url and your branch name. For example, `pynumaflow = {git = "https://github.com/chromevoid/numaflow-python", rev = "test-branch"}`
4. Run `poetry update -vv` from this `developer_guide` folder. You should get a `poetry.lock` file.
5. Update your `example.py` in the `Makefile` as needed.
6. Run `make image` to build your image.
7. Now you have the image with your customized example and your code change to test in a numaflow pipeline. Example pipeline `pipeline-numaflow.yaml` is also provided in this `developer_guide` folder. Please check [numaflow](https://numaflow.numaproj.io/) for more details.

Each example directory has a Makefile which can be used to build, tag, and push images. In most cases, the `image-push` target should be used.
However, `buildx`, which is used to support multiple architectures, does not make a built image available locally. In the case that a developer
wants this functionality, they can use the `image` target.

After making changes to the SDK, if you want to build all the example images at once, in the root directory you can run:
```shell
./hack/update_examples.sh -bp -t <tag>
```
The default tag is `stable`, but it is recommended you specify your own for testing purposes, as the Github Actions CI uses the `stable` tag. Note: do not forget to clean up testing tags
in the registry, i.e. delete them, once you are done testing.

You can alternatively build a specific example image by running the following in the root directory and providing the path to the Dockerfile (relative to root):
```shell
./hack/update_examples.sh -bpe <path> -t <tag>
```
This is essentially equivalent to running `make image-push TAG=<tag>` in the example directory itself.

### Deploying

Once you have confirmed that your changes pass local testing:
1. Revert the `pyproject.toml` file to its previous state, i.e. before you updated it with your forked repo and branch
2. Create a PR for your changes

Once the PR has been merged it is important to update the pynumaflow dependency to point to the merged commit SHA. Thus,
before you delete/leave your branch:

1.
```shell
./hack/update_examples.sh -u <commit-sha>
```

2.
```shell
./hack/update_examples.sh -bp
```

The above commands will update the pynumaflow dependency to the specified commit SHA, and build and push the image, respectively,
across all example directories. Since we do not want to flood the commit history with dependency updates, it is not necessary
to create a second PR with these changes. Thus, although in the remote repository it will still show the old commit SHA
the pipelines will be using the most up to date image.

### Releasing

In order to resolve the dependency drift issue mentioned above (what is actually being used vs what is displayed in the `pyproject.toml`
files of the examples), everytime a new version of pynumaflow is released, i.e. a release branch is merged, a workflow will be
triggered to update the `pyproject.toml` files to the newest version and then build, tag, and push the example images. This way,
the latest version will always be shown, while the latest commit sha of that version will always be in use.
