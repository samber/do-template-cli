
# CLI boilerplate showcasing github.com/samber/do

![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.23-%23007d9c)
![Build Status](https://github.com/samber/do-template-cli/actions/workflows/test.yml/badge.svg)
[![Go report](https://goreportcard.com/badge/github.com/samber/do-template-cli)](https://goreportcard.com/report/github.com/samber/do)
[![License](https://img.shields.io/github/license/samber/do-template-cli)](./LICENSE)

**⚙️ Fork this repository and start your new project with `do` dependency injection.**

A comprehensive CLI template project demonstrating the full power of the `github.com/samber/do` dependency injection library. This project implements a complete data processing application with type-safe dependency injection, modular architecture, and real-world concerns.

Perfect as a starting point for new Go projects or as a learning resource for understanding dependency injection patterns in Go applications.

**See also:**

- [do-template-api](https://github.com/samber/do-template-api)
- [do-template-worker](https://github.com/samber/do-template-worker)

## 🚀 Install

Clone the repo and install dependencies:

```bash
git clone --depth 1 --branch main https://github.com/samber/do-template-cli.git your-project-name
cd your-project-name

docker compose up -d
make deps
make deps-tools
```

## 💡 Features

- **Type-safe dependency injection** - Service registration and resolution using `samber/do`
- **Modular architecture** - Clean separation of concerns with dependency tree visualization
- **CLI framework integration** - Built with Cobra for powerful command-line interfaces
- **Configuration management** - Environment-based configuration with dependency injection
- **Data processing pipeline** - Complete example with CSV/JSON processing and file I/O
- **Repository pattern** - Data access layer with injected dependencies
- **Service layer** - Business logic with proper dependency management
- **Application lifecycle** - Health checks and graceful shutdown handling
- **Comprehensive error handling** - Structured logging and error management
- **Production-ready** - Ready to fork and customize for your next project
- **Extensive documentation** - Inline comments explaining every `do` library feature

## 🚀 Contributing

```sh
# install deps
make deps
make deps-tools

# compile
make build

# build with hot-reload
make watch-run

# test with hot-reload
make watch-test
```

## 🤠 `do` documentation

- [GoDoc: https://godoc.org/github.com/samber/do/v2](https://godoc.org/github.com/samber/do/v2)
- [Documentation](https://do.samber.dev/docs/getting-started)

## 💫 Show your support

Give a ⭐️ if this project helped you!

[![GitHub Sponsors](https://img.shields.io/github/sponsors/samber?style=for-the-badge)](https://github.com/sponsors/samber)

## 📝 License

Copyright © 2025 [Samuel Berthe](https://github.com/samber).

This project is [MIT](./LICENSE) licensed.
