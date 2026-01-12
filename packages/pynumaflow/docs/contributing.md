# Contributing

Thank you for your interest in contributing to pynumaflow! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- [Poetry](https://python-poetry.org/) for dependency management
- Git

### Clone and Install

```bash
# Clone the repository
git clone https://github.com/numaproj/numaflow-python.git
cd numaflow-python/packages/pynumaflow

# Install dependencies
make setup
```

This will install all development dependencies including testing and linting tools.

### Verify Installation

```bash
# Run tests
make test

# Run linting
make lint
```

## Code Style

We use [Black](https://black.readthedocs.io/) for code formatting and [Ruff](https://github.com/astral-sh/ruff) for linting.

### Format Code

```bash
make format
```

### Lint Code

```bash
make lint
```

### Pre-commit Hooks

We recommend setting up pre-commit hooks to automatically format and lint code before commits:

```bash
pre-commit install
```

## Testing

### Run All Tests

```bash
make test
```

### Run Specific Tests

```bash
poetry run pytest tests/test_mapper.py -v
```

### Test Coverage

```bash
poetry run pytest tests/ --cov=pynumaflow --cov-report=html
```

## Project Structure

```
packages/pynumaflow/
├── pynumaflow/           # Main package
│   ├── mapper/           # Map UDF implementation
│   ├── reducer/          # Reduce UDF implementation
│   ├── mapstreamer/      # MapStream implementation
│   ├── batchmapper/      # BatchMap implementation
│   ├── sourcer/          # Source implementation
│   ├── sinker/           # Sink implementation
│   ├── sourcetransformer/# SourceTransform implementation
│   ├── sideinput/        # SideInput implementation
│   ├── reducestreamer/   # ReduceStream implementation
│   ├── accumulator/      # Accumulator implementation
│   ├── proto/            # Protocol buffer definitions
│   ├── shared/           # Shared utilities
│   └── types.py          # Common type definitions
├── tests/                # Test files
├── examples/             # Example implementations
├── docs/                 # Documentation source
├── pyproject.toml        # Project configuration
└── Makefile              # Development commands
```

## Making Changes

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Write clear, documented code
- Follow the existing code style
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
make test
make lint
```

### 4. Commit Your Changes

Write clear commit messages:

```bash
git commit -m "Add feature: description of your change"
```

### 5. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Protocol Buffers

If you need to modify protocol buffer definitions:

1. Edit the `.proto` files in `pynumaflow/proto/`
2. Regenerate Python files:

```bash
make proto
```

## Documentation

### Local Preview

```bash
# Install docs dependencies
poetry install --with docs

# Serve locally
make docs-serve
```

Visit `http://localhost:8000` to preview.

### Writing Documentation

- Documentation source files are in `docs/`
- Use Markdown with MkDocs extensions
- Include code examples
- Keep explanations clear and concise

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass (`make test`)
- [ ] Code is formatted (`make lint`)
- [ ] Documentation is updated (if applicable)
- [ ] Commit messages are clear

### PR Description

Include:

- What the change does
- Why the change is needed
- How to test the change
- Any breaking changes

### Review Process

1. A maintainer will review your PR
2. Address any feedback
3. Once approved, your PR will be merged

## Reporting Issues

### Bug Reports

Include:

- Python version
- pynumaflow version
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages (if any)

### Feature Requests

Include:

- Use case description
- Proposed solution
- Alternatives considered

## Code of Conduct

Please be respectful and constructive in all interactions. We're all here to build great software together.

## Getting Help

- [GitHub Issues](https://github.com/numaproj/numaflow-python/issues)
- [Numaflow Slack](https://numaproj.slack.com)
- [Numaflow Documentation](https://numaflow.numaproj.io/)

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
