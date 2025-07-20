# Contributing

[poetry](https://python-poetry.org/) is used to manage dependencies and packaging.

Common commands:
- `poetry install` to install dependencies.
- `poetry update` to update dependencies and `poetry.lock` file.
- `poetry build` to build the package.
- `poetry publish` to publish the package to PyPI.

## PyPI

To publish to PyPI, configure `poetry` to use your credentials following
[these instructions](https://python-poetry.org/docs/repositories/#configuring-credentials).

## Testing

```bash
./bin/test
```
