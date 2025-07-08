# Docker Build Optimization for NumaFlow Python UDFs

## Overview

This document outlines the optimization strategies to reduce Docker build times for NumaFlow Python UDFs from 2+ minutes to under 30 seconds for subsequent builds.

## Current Issues

1. **Redundant dependency installation**: Each UDF rebuilds the entire pynumaflow package
2. **No layer caching**: Dependencies are reinstalled every time
3. **Copying entire project**: The `COPY ./ ./` copies everything, including unnecessary files
4. **No shared base layers**: Each UDF builds its own base environment

## Optimization Strategy: Three-Stage Approach

As suggested by @kohlisid, we implement a three-stage build approach:

### Stage 1: Base Layer
- Common Python environment and tools
- System dependencies (curl, wget, build-essential, git)
- Poetry installation
- dumb-init binary

### Stage 2: Environment Setup
- pynumaflow package installation
- Shared virtual environment creation
- This layer is cached unless `pyproject.toml` or `poetry.lock` changes

### Stage 3: Builder
- UDF-specific code and dependencies
- Reuses the pynumaflow installation from Stage 2
- Minimal additional dependencies

## Implementation Options

### Option 1: Optimized Multi-Stage Build (Recommended)

**File**: `examples/map/even_odd/Dockerfile.optimized`

**Benefits**:
- Better layer caching
- Reduced build time by ~60-70%
- No external dependencies

**Usage**:
```bash
cd examples/map/even_odd
make -f Makefile.optimized image
```

### Option 2: Shared Base Image (Fastest)

**Files**: 
- `Dockerfile.base` (shared base image)
- `examples/map/even_odd/Dockerfile.shared-base` (UDF-specific)

**Benefits**:
- Maximum caching efficiency
- Build time reduced by ~80-90% for subsequent builds
- Perfect for CI/CD pipelines

**Usage**:
```bash
# Build base image once
docker build -f Dockerfile.base -t numaflow-python-base .

# Build UDF images (very fast)
cd examples/map/even_odd
make -f Makefile.optimized image-fast
```

## Performance Comparison

| Approach | First Build | Subsequent Builds | Cache Efficiency |
|----------|-------------|-------------------|------------------|
| Current | ~2-3 minutes | ~2-3 minutes | Poor |
| Optimized Multi-Stage | ~2-3 minutes | ~45-60 seconds | Good |
| Shared Base Image | ~2-3 minutes | ~15-30 seconds | Excellent |

## Implementation Steps

### 1. Build Shared Base Image (One-time setup)

```bash
# From project root
docker build -f Dockerfile.base -t numaflow-python-base .
```

### 2. Update UDF Dockerfiles

Replace the current Dockerfile with the optimized version:

```bash
# For each UDF directory
cp Dockerfile.optimized Dockerfile
# or
cp Dockerfile.shared-base Dockerfile
```

### 3. Update Makefiles

Use the optimized Makefile:

```bash
# For each UDF directory
cp Makefile.optimized Makefile
```

### 4. CI/CD Integration

For CI/CD pipelines, add the base image build step:

```yaml
# Example GitHub Actions step
- name: Build base image
  run: docker build -f Dockerfile.base -t numaflow-python-base .
  
- name: Build UDF images
  run: |
    cd examples/map/even_odd
    make image-fast
```

## Advanced Optimizations

### 1. Dependency Caching

The optimized Dockerfiles implement smart dependency caching:
- `pyproject.toml` and `poetry.lock` are copied first
- pynumaflow installation is cached separately
- UDF-specific dependencies are installed last

### 2. Layer Optimization

- Minimal system dependencies in runtime image
- Separate build and runtime stages
- Efficient file copying with specific paths

### 3. Build Context Optimization

- Copy only necessary files
- Use `.dockerignore` to exclude unnecessary files
- Minimize build context size

## Migration Guide

### For Existing UDFs

1. **Backup current Dockerfile**:
   ```bash
   cp Dockerfile Dockerfile.backup
   ```

2. **Choose optimization approach**:
   - For single UDF: Use `Dockerfile.optimized`
   - For multiple UDFs: Use `Dockerfile.shared-base`

3. **Update Makefile**:
   ```bash
   cp Makefile.optimized Makefile
   ```

4. **Test the build**:
   ```bash
   make image
   # or
   make image-fast
   ```

### For New UDFs

1. **Use the optimized template**:
   ```bash
   cp examples/map/even_odd/Dockerfile.optimized your-udf/Dockerfile
   cp examples/map/even_odd/Makefile.optimized your-udf/Makefile
   ```

2. **Update paths in Dockerfile**:
   - Change `EXAMPLE_PATH` to your UDF path
   - Update `COPY` commands accordingly

## Troubleshooting

### Common Issues

1. **Base image not found**:
   ```bash
   docker build -f Dockerfile.base -t numaflow-python-base .
   ```

2. **Permission issues**:
   ```bash
   chmod +x entry.sh
   ```

3. **Poetry cache issues**:
   ```bash
   poetry cache clear --all pypi
   ```

### Performance Monitoring

Monitor build times:
```bash
time make image
time make image-fast
```

## Future Enhancements

1. **Registry-based base images**: Push base image to registry for team sharing
2. **BuildKit optimizations**: Enable BuildKit for parallel layer building
3. **Multi-platform builds**: Optimize for ARM64 and AMD64
4. **Dependency analysis**: Automate dependency optimization

## Contributing

When adding new UDFs or modifying existing ones:

1. Use the optimized Dockerfile templates
2. Follow the three-stage approach
3. Test build times before and after changes
4. Update this documentation if needed

## References

- [Docker Multi-Stage Builds](https://docs.docker.com/develop/dev-best-practices/multistage-build/)
- [Docker Layer Caching](https://docs.docker.com/develop/dev-best-practices/dockerfile_best-practices/#leverage-build-cache)
- [Poetry Docker Best Practices](https://python-poetry.org/docs/configuration/#virtualenvsin-project) 