# Getting Started with DieselDB — Good First Issues

Welcome to DieselDB! This guide will help you get started with contributing to the project through our "good first issue" program. These issues are designed specifically for new contributors and typically take 1-2 hours to complete.

## 🎯 What Are Good First Issues?

Good first issues are beginner-friendly contributions that help you learn the codebase while making valuable improvements to the project. They're:

- **Well-scoped**: Focused on specific, manageable tasks
- **Well-documented**: Clear requirements and expectations
- **Well-supported**: Community ready to help answer questions
- **Valuable**: Address real needs in the project

## 📝 Current Good First Issues

### 1. Spring Boot Embedded Mode Integration
**Files to create**: `examples/spring-boot-embedded/`
**Skills needed**: Java, Spring Boot, Maven
**What you'll learn**: Database integration patterns, Spring configuration

### 2. Hibernate Integration Example
**Files to create**: `examples/hibernate-integration/`
**Skills needed**: Java, Hibernate, JPA, Maven
**What you'll learn**: ORM integration, JPA mapping, database drivers

### 3. SQL Keyword Error Message Enhancement
**Files to modify**: SQL parser classes
**Skills needed**: Java, SQL parsing, error handling
**What you'll learn**: SQL language processing, error handling patterns

### 4. JMH Benchmark for Indexed Queries
**Files to create**: JMH benchmark classes
**Skills needed**: Java, performance testing, JMH
**What you'll learn**: Performance measurement, query optimization, benchmarking

### 5. README Translation to English
**Files to modify**: `README.md`
**Skills needed**: Technical writing, bilingual skills
**What you'll learn**: Project documentation, internationalization

## 🚀 Getting Started Guide

### 1. Fork and Clone
```bash
# Fork the repository on GitHub
git clone https://github.com/YOUR_USERNAME/dieseldb.git
cd dieseldb
```

### 2. Set Up Your Environment
```bash
# Verify Java 21 is installed
java -version

# Verify Maven is installed
mvn -version

# Build the project
make compile

# Run tests to ensure everything works
make test
```

### 3. Choose Your Issue
Browse the [good first issues](https://github.com/Reider85/dieseldb/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) and pick one that interests you.

### 4. Create Your Branch
```bash
git checkout -b feature/issue-name
# Example: git checkout -b feature/spring-boot-example
```

### 5. Understand the Requirements
Read the issue description carefully and ask questions if anything is unclear.

### 6. Implement Your Solution
Start coding! Follow the project's coding standards and add appropriate tests.

### 7. Test Your Changes
```bash
# Run relevant tests
make test

# Run specific test if applicable
make test-one T=YourTestClass#yourTestMethod
```

### 8. Commit Your Work
```bash
git add .
git commit -m "feat: add spring boot embedded example"
```

### 9. Create a Pull Request
Push your branch and create a pull request with:
- Clear title
- Detailed description of what you did
- Links to any related issues
- Test results

## 💡 Tips for Success

### Before Starting
- **Read the documentation**: Check the [README](https://github.com/Reider85/dieseldb/blob/main/README.md) and [CONTRIBUTING.md](https://github.com/Reider85/dieseldb/blob/main/CONTRIBUTING.md)
- **Search existing issues**: Avoid duplicate work
- **Ask questions**: Don't hesitate to ask in discussions

### While Coding
- **Follow coding standards**: Match the existing code style
- **Add tests**: Ensure your changes don't break existing functionality
- **Keep it simple**: Focus on the specific issue requirements
- **Document your changes**: Add comments where needed

### Testing Your Work
- **Run the full test suite**: `make test`
- **Check specific areas**: Run tests related to your changes
- **Performance considerations**: Ensure your changes don't degrade performance

### Communication
- **Be responsive**: Respond to review comments promptly
- **Be patient**: Code review takes time
- **Be collaborative**: Work with reviewers to improve your contribution

## 🤝 Getting Help

### Resources
- **Documentation**: [DieselDB Docs](https://reider85.github.io/dieseldb/)
- **Discussions**: [Community Forum](https://github.com/Reider85/dieseldb/discussions)
- **Issues**: [Bug Reports & Feature Requests](https://github.com/Reider85/dieseldb/issues)
- **Code**: [Source Code](https://github.com/Reider85/dieseldb)

### How to Ask for Help
1. **Search first**: Check existing discussions and issues
2. **Be specific**: Provide clear details about your problem
3. **Include context**: Your environment, steps taken, error messages
4. **Show your work**: Share what you've tried so far

### Community Channels
- **GitHub Discussions**: Best for general questions and discussions
- **GitHub Issues**: For bug reports and feature requests
- **Pull Request Reviews**: For code feedback and collaboration

## 🎯 Next Steps After Your First Contribution

### Intermediate Contributions
Once you've completed a good first issue, consider:
- **More complex features**: Take on issues labeled `help wanted`
- **Documentation improvements**: Help with tutorials and guides
- **Bug fixes**: Contribute to issue resolution
- **Performance optimizations**: Work on speed improvements

### Advanced Contributions
Experienced contributors can:
- **Architecture improvements**: Help with design decisions
- **New features**: Implement major capabilities
- **Community leadership**: Help guide new contributors
- **Project governance**: Participate in planning discussions

## 🏆 Recognition

### Contributor Recognition
- **GitHub Profile**: Your contributions show on your profile
- **Project Contributors**: Listed in project documentation
- **Community Recognition**: Acknowledged in discussions and releases

### Skill Development
Through contributing, you'll gain:
- **Database knowledge**: Understanding of database engines
- **Java expertise**: Advanced Java programming skills
- **Open source experience**: Real-world development practices
- **Community skills**: Collaboration and communication

## 🙏 Thank You

Thank you for considering contributing to DieselDB! Your participation helps:

- **Improve the project**: Every contribution makes DieselDB better
- **Build the community**: More contributors mean more knowledge sharing
- **Support the ecosystem**: Users benefit from your improvements
- **Advance database technology**: Open source drives innovation

**Remember**: ⭐ Star this repo if you find DieselDB useful — it helps others discover it!

---

**Ready to start? Pick a good first issue and begin your journey with DieselDB!**