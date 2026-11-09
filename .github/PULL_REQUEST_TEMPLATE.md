# Pull Request Template for DieselDB

## Description
Please provide a clear and concise description of what this pull request accomplishes.

## Changes Made
- [ ] **Bug Fix**: Fixed a specific issue or bug
- [ ] **Feature**: Added new functionality
- [ ] **Documentation**: Updated or added documentation
- [ ] **Refactor**: Improved code structure without changing behavior
- [ ] **Performance**: Optimized for better performance
- [ ] **Test**: Added or improved tests
- [ ] **Other**: (please specify)

## Type of Change
- [ ] **Breaking Change**: This change will break existing functionality
- [ ] **Non-Breaking Change**: This change maintains backward compatibility
- [ ] **Documentation Only**: No code changes

## Testing
- [ ] I have run the relevant test gates:
  - [ ] `make compile` - compilation check
  - [ ] `make test` - fast test suite
  - [ ] `make test-core` - integration tests
  - [ ] `make timing-quick` - performance regression test (if applicable)
  - [ ] Other: (please specify)

- [ ] I have added tests to cover my changes
- [ ] I have manually tested my changes

## Related Issue
Closes # (issue number) or Resolves # (if already closed)

## Checklist
- [ ] My code follows the project's [Code Style guidelines](CONTRIBUTING.md#code-style)
- [ ] I have updated the [Changelog.md](Changelog.md) if this is a user-facing change
- [ ] I have updated the [README.md](README.md) if this changes user-facing functionality
- [ ] I have updated the [PROMPT_STATUS.md](PROMPT_STATUS.md) if this is part of a prompt-driven workflow
- [ ] All new and existing tests pass
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas

## Additional Context
Please provide any additional context, screenshots, or examples that help explain your changes.

## How to Test
Provide step-by-step instructions on how to test this pull request:

```bash
# Example test steps
git checkout feature-branch
make compile
make test
# Test the new functionality
./scripts/make.ps1 test-one T=NewFeatureTest
```

## Screenshots (if applicable)
Add screenshots here to demonstrate the changes.

## Performance Impact
- [ ] No performance impact expected
- [ ] Performance improvement expected
- [ ] Performance regression possible (please explain)
- [ ] Performance impact unknown (needs benchmarking)

## Dependencies
- [ ] No new dependencies added
- [ ] New dependencies added (please list):
  - 
- [ ] Dependencies updated (please list):
  - 

## Future Considerations
Any future work or considerations that should be noted?

---

**Thank you for your contribution to DieselDB!** 🚀

Remember to check our [Contributing Guide](CONTRIBUTING.md) for more information on our development process.