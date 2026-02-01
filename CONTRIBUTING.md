# Contributing to Motiva

Thank you for your interest in contributing to Motiva! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Getting started](#getting-started)
- [Submitting changes](#submitting-changes)
- [Development guidelines](#development-guidelines)

## Getting started

Motiva is a Rust reimplementation of Yente/nomenklatura for matching entities against sanctions lists. The project consists of three workspace crates:

- **libmotiva** (`crates/libmotiva`): Core matching library with algorithms and scoring
- **motiva** (`crates/motiva`): REST API interfacing with `libmotiva`
- **libmotiva-macros** (`crates/macros`): Procedural macros for the library

Before contributing, please familiarize yourself with the project structure and read the README.md for context on the project's goals and scope.

## Submitting changes

### Opening an issue first

**We strongly recommend opening an issue before starting work on a contribution.** This allows us to:

 - Discuss whether the change aligns with the project's goals and scope
 - Talk about design approaches and implementation strategies
 - Avoid duplicate work if someone else is already working on something similar
 - Ensure the change is needed and makes sense for the project

For bug fixes, please describe:

 - The bug you encountered
 - Steps to reproduce
 - Expected vs actual behavior

For features or enhancements, please describe:

 - The problem you're trying to solve
 - Your proposed solution or approach
 - Any alternative approaches you considered

This discussion helps ensure your contribution will be accepted and saves everyone time.

### Before submitting

 1. Ensure all tests pass: `cargo test`
 2. Ensure code is formatted: `cargo fmt`
 3. Ensure clippy passes: `cargo clippy --all-features -- -D warnings`
 4. Ensure code compiles: `cargo check --all-features`
 5. Write or update tests for your changes
 6. Update documentation if needed

### Commit message

- Use clear, concise first lines for commit messages
- Provide additional context in the commit body if needed

## Development guidelines

For any change to the scoring algorithms, it is required to provide full
examples of a noticeable mismatch between the score produced by Yente and that
of motiva. Differences due to string latinization without the ICU feature will
not be accepted. All changes to scoring algorithms must be fully covered by
regression tests.

### Testing

 - Add unit tests in the same file as the code being tested
 - Add integration tests in `crates/motiva/tests/` for end-to-end scenarios when appropriate
 - For matching algorithms, include Python interop tests comparing against nomenklatura
 - Scores should be within epsilon (0.01) of the reference implementation

### AI-generated code

Whether using AI tools or other resources, contributors are expected to verify
their changes work correctly and align with the project's guidelines. This helps
maintain code quality and respects everyone's time.

If you use AI tools to assist with development, please ensure you:

 - Review and test the changes thoroughly
 - Can explain and discuss your implementation choices
 - Verify the code follows the project's guidelines and conventions
 - Confirm the solution addresses the actual problem

 Pull requests that are obviously entirely authored by LLMs where the submitter
 is not able to justify their understanding of the work will not be approved. PR
 submitted or commented on by LLMs will be closed immediately. 

## Questions or issues?

If you have questions or run into issues:

 1. Check existing issues on GitHub
 2. Open a new issue with a clear description of your question or problem

## License

By contributing to Motiva, you agree that your contributions will be licensed under the MIT License. See the LICENSE file for details.

Thank you for contributing to Motiva!
