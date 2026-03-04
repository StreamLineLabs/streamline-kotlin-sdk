# Contributing to Streamline Kotlin SDK

Thank you for your interest in contributing to the Streamline Kotlin SDK! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m "Add my feature"`)
6. Push to your fork (`git push origin feature/my-feature`)
7. Open a Pull Request

## Prerequisites

- JDK 17 or later
- Gradle 8.x (or use the included Gradle wrapper when available)

## Development Setup

```bash
# Clone your fork
git clone https://github.com/<your-username>/streamline-kotlin-sdk.git
cd streamline-kotlin-sdk

# Build the project
./gradlew build

# Run tests
./gradlew test
```

## Code Style

- Follow Kotlin coding conventions
- Use meaningful variable and function names
- Add KDoc for public APIs
- Keep functions focused and short

## Pull Request Guidelines

- Write clear commit messages
- Add tests for new functionality
- Update documentation if needed
- Ensure `./gradlew build` passes before submitting

## Reporting Issues

- Use the **Bug Report** or **Feature Request** issue templates
- Search existing issues before creating a new one
- Include reproduction steps for bugs

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](https://github.com/streamlinelabs/.github/blob/main/CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.

