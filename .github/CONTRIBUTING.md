# Contributing Guidelines

Thank you for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/Snehabankapalli/real-time-fintech-pipeline-kafka-spark-snowflake
cd real-time-fintech-pipeline-kafka-spark-snowflake

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt
pip install flake8 mypy black isort pytest pytest-cov sqlfluff
```

## Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make changes
4. Write/update tests
5. Run quality checks (see below)
6. Commit: `git commit -m 'feat: add amazing feature'`
7. Push: `git push origin feature/amazing-feature`
8. Open a Pull Request

## Quality Standards

Before submitting a PR, ensure all checks pass:

```bash
# Format
black src/ tests/
isort src/ tests/

# Lint
flake8 src/ tests/
mypy src/

# Test
pytest tests/ -v --cov=src --cov-fail-under=80

# SQL lint
sqlfluff lint dbt/models/ --dialect snowflake
```

## Commit Format

```
<type>: <description>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf`, `ci`

## Pull Request Process

1. Update documentation if APIs change
2. Add tests for new features (80%+ coverage required)
3. Ensure all CI checks pass
4. Request review from maintainers
5. Address feedback before merge

## Code Style

- Python: PEP 8 (enforced by Black)
- SQL: Snowflake dialect (via sqlfluff)
- No hardcoded secrets — use environment variables
- Error handling at every level
- Functions under 50 lines

## Questions?

Open an issue or contact [@Snehabankapalli](https://github.com/Snehabankapalli)
