# Developer Guide

This document explains the development process for the Numaflow Python SDK.

### Testing

1. Install [Poetry](https://python-poetry.org/docs/) before starting your test. Make sure you have the correct Python version
2. Make your SDK changes
3. Update the `example.py` inside the same example directory if needed 
4. Run `make image` inside the same example directory to build your image
5. Now that you have the image with your customized example and code change, you can test it in a Numaflow pipeline. Example pipelines, `pipeline.yaml`, are also provided in most of the example directories.
Please check [Numaflow](https://numaflow.numaproj.io/) for more details

Each example directory has a Makefile which can be used to build, tag, and push images.
If you want to build and tag the image and then immediately push it to quay.io, use the `image-push` target.
If you want to build and tag a local image without pushing, use the `image` target.

If you would like to build and push a specific example, you can do so by running:
```shell
./.hack/update_examples.sh -bpe <example-directory-path> -t <tag>
```
This is essentially equivalent to running `make image-push TAG=<tag>` in the example directory itself.
The default tag is `stable`, but it is recommended you specify your own for testing purposes, as the Github Actions CI uses the `stable` tag.
This consistent tag name is used so that the tags in the [E2E test pipelines](https://github.com/numaproj/numaflow/tree/main/test) do not need to be updated each time an SDK change is made.

Note: before running the script, ensure that through the CLI, you are logged into quay.io.

### Deploying

After confirming that your changes pass local testing:
1. Clean up testing artifacts (remove any test images on quay.io, etc.)
2. Create a PR for your changes. Once your PR has been merged, a Github Actions workflow (`Docker Publish`) will be triggered, to build, tag (with `stable`), and push
all example images. This ensures that all example images are using the most up-to-date version of the SDK, i.e. the one including your
changes

### Adding a New Example

If you add a new example, in order for it to be used by the `Docker Publish` workflow, add its path
to the `example_directories` matrix in `.github/workflows/build-push.yaml`.