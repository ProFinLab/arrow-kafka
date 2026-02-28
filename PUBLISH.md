# Publishing arrow-kafka-pyo3 to PyPI

## Prerequisites

- Rust toolchain, maturin, Python 3.10+
- PyPI account (and TestPyPI for dry-run)

## 1. Bump version

Edit `crates/arrow-kafka-pyo3/Cargo.toml`: set `version = "0.1.0"` (or next semver).

## 2. Build wheels

From repo root (or from `crates/arrow-kafka-pyo3`):

```bash
cd crates/arrow-kafka-pyo3
maturin build --release
```

Wheels are in `target/wheels/`.

## 3. TestPyPI (recommended first)

```bash
# Create API token at https://test.pypi.org/manage/account/token/
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-...

maturin upload -r testpypi target/wheels/*.whl
pip install -i https://test.pypi.org/simple/ arrow-kafka-pyo3
python -c "from arrow_kafka_pyo3 import ArrowKafkaSink; print('OK')"
```

## 4. PyPI (production)

```bash
# Create API token at https://pypi.org/manage/account/token/
maturin upload target/wheels/*.whl
```

## 5. Nanoquant dependency

After publishing, in Nanoquant add to the Python env that uses Kafka sink:

```bash
pip install arrow-kafka-pyo3>=0.1.0
```

Or in `pyproject.toml` / `requirements*.txt`:

```
arrow-kafka-pyo3>=0.1.0
```
