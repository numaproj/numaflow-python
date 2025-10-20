## Development Setup

```bash
# new venv
uv venv

# activate venv
source venv/bin/activate

uv pip install maturin

# install dependencies
uv sync
```

### Testing

```bash
make test
```

### HOWTO create .whl

Go to `pynumaflow-lite` (top level) directory and run the below command.

```bash
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build -i python3.11 --release 
```

This will create the `wheel` file in `target/wheels/` directory. You should copy it over to where we
are writing the python code referencing this library.

e.g.,

```bash
cp target/wheels/pynumaflow_lite-0.1.0-cp311-cp311-manylinux_2_17_aarch64.manylinux2014_aarch64.whl manifests/simple-async-map/
```