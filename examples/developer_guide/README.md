# Developer Guide

This example is for numaflow-python contributors/developers. The example includes how to use your branch to build the UDF image to test your code change before submitting a PR. 

1. Install [Poetry](https://python-poetry.org/docs/) before starting your test. Make sure you have the correct python version.
2. Push your code change to your branch.
3. Update the `pynumaflow` in `pyproject.tomal` file with your (forked) repo url and your branch name. For example, `pynumaflow = {git = "https://github.com/chromevoid/numaflow-python", rev = "test-branch"}`
4. Run `poetry update -vv` from this `developer_guide` folder. You should get a `poetry.lock` file.
5. Update your `example.py` and the `image name` in `Makefile` as needed.
6. Run `make image` to build your image.
7. Now you have the image with your customized example and your code change to test in the numaflow pipeline. Example pipeline `pipeline-numaflow.yaml` is also provided in this `developer_guide` folder. Please check [numaflow](https://numaflow.numaproj.io/) for more details.
