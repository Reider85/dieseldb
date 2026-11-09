# Good First Issues - DieselDB

Welcome to DieselDB! This file contains templates for "good first issue" contributions designed for new contributors. Each issue typically takes 1-2 hours to complete and helps you understand the codebase.

## Issue Templates

### 1. Add example: DieselDB + Spring Boot embedded mode

**Issue**: Create a Spring Boot example that uses DieselDB as an embedded database

**What to do**:
- Create a new Spring Boot application in `examples/spring-boot-embedded/`
- Add dependency to `diesel-jdbc` driver
- Create a simple entity class and Spring Data repository
- Add configuration to use DieselDB as the embedded database
- Include basic tests to verify the integration

**Complexity**: Beginner  
**Expected time**: 1-2 hours  
**Files to create**: `examples/spring-boot-embedded/pom.xml`, `examples/spring-boot-embedded/src/main/java/...`, `examples/spring-boot-embedded/src/test/java/...`

### 2. Add example: DieselDB + JDBC + Hibernate

**Issue**: Create a Hibernate example that uses DieselDB as the underlying database

**What to do**:
- Create a new Hibernate example in `examples/hibernate-integration/`
- Add dependencies to `diesel-jdbc`, Hibernate, and JPA
- Create JPA entity classes mapping to DieselDB tables
- Configure Hibernate to connect to DieselDB
- Add integration tests to verify CRUD operations work correctly

**Complexity**: Intermediate  
**Expected time**: 1.5-2.5 hours  
**Files to create**: `examples/hibernate-integration/pom.xml`, entity classes, Hibernate configuration, test files

### 3. Improve error message for unknown SQL keyword

**Issue**: Enhance error messages when DieselDB encounters unknown SQL keywords

**What to do**:
- Find the SQL parser component that handles keyword validation
- Improve the error message to include the unknown keyword and suggestions for similar valid keywords
- Add unit tests to verify the improved error messages
- Consider adding a "did you mean?" feature for common typos

**Complexity**: Intermediate  
**Expected time**: 1-2 hours  
**Files to modify**: SQL parser classes, test files

### 4. Add JMH benchmark for SELECT WHERE on indexed column

**Issue**: Create a JMH benchmark to measure performance of indexed SELECT queries

**What to do**:
- Create a new JMH benchmark class in `src/benchmark/java/`
- Set up test data with indexed columns
- Benchmark SELECT queries with WHERE clauses on indexed vs non-indexed columns
- Measure and report query performance metrics
- Add configuration for different data set sizes

**Complexity**: Intermediate  
**Expected time**: 2-3 hours  
**Files to create**: JMH benchmark class, test data setup

### 5. Translate README sections to English

**Issue**: Translate remaining Russian sections in README.md to English

**What to do**:
- Identify Russian sections in README.md that need English translation
- Translate technical content accurately while preserving meaning
- Ensure both English and Russian versions are available
- Update the table of contents to reference both versions

**Complexity**: Beginner  
**Expected time**: 1-1.5 hours  
**Files to modify**: README.md

---

## Getting Started

1. **Fork the repository**: Create your own fork of DieselDB
2. **Clone locally**: `git clone https://github.com/your-username/dieseldb.git`
3. **Set up environment**: Ensure JDK 21 and Maven are installed
4. **Pick an issue**: Choose one of the templates above
5. **Create branch**: `git checkout -b feature/issue-name`
6. **Make changes**: Implement the solution
7. **Add tests**: Include relevant unit/integration tests
8. **Test thoroughly**: Run `make test` to ensure everything works
9. **Submit PR**: Open a pull request with clear description

## Labels

Use these labels when creating issues:
- `good first issue` - For beginner-friendly contributions
- `help wanted` - For issues that need community help
- `documentation` - For documentation improvements
- `example` - For new example code
- `benchmark` - For performance testing

## Support

- **Discussions**: [Ask questions in our community forum](https://github.com/Reider85/dieseldb/discussions)
- **Issues**: [Report bugs or request features](https://github.com/Reider85/dieseldb/issues)
- **Chat**: Join our community discussions for real-time help

We welcome all contributions and are happy to help you get started!