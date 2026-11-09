# Good First Issues for DieselDB

Welcome to DieselDB! Here are some beginner-friendly issues that are perfect for new contributors. Each typically takes 1-2 hours to complete and helps you get familiar with the codebase.

## 🎯 Getting Started

### 1. Add example: DieselDB + Spring Boot embedded mode
**Issue:** Create a simple example showing how to embed DieselDB in a Spring Boot application  
**Complexity:** Beginner  
**Expected time:** 1-2 hours  
**Context:** Spring Boot is a popular framework for Java applications. Many users would benefit from seeing how to embed DieselDB as an embedded database in Spring Boot.  
**What to do:**
- Create a new Spring Boot project structure
- Add DieselDB dependency
- Configure DataSource to use DieselDB
- Create a simple REST API that uses the database
- Add basic tests
- Document the setup in README.md

**Link to relevant code:** Look at existing Spring Boot examples in the repository or create a new `examples/spring-boot-embedded` directory.

### 2. Add example: DieselDB + JDBC + Hibernate
**Issue:** Create an example showing integration between DieselDB and Hibernate ORM  
**Complexity:** Intermediate  
**Expected time:** 2-3 hours  
**Context:** Hibernate is the most popular JPA implementation in Java. Users want to know how to use DieselDB as a backend for Hibernate applications.  
**What to do:**
- Create a new Maven project structure
- Add DieselDB and Hibernate dependencies
- Configure Hibernate to use DieselDB dialect
- Create entity classes and repository
- Add integration tests
- Document the setup and configuration

**Link to relevant code:** Look at existing Hibernate configurations and create a new `examples/hibernate-integration` directory.

### 3. Improve error message for unknown SQL keyword
**Issue:** Enhance error messages when an unknown SQL keyword is encountered  
**Complexity:** Beginner  
**Expected time:** 1 hour  
**Context:** When users make typos in SQL queries, the current error message might not be helpful. We should provide better suggestions.  
**What to do:**
- Find the SQL parser code that handles unknown keywords
- Implement a fuzzy matching algorithm to suggest similar keywords
- Improve the error message to include suggestions
- Add tests for the new error messages
- Update documentation if needed

**Link to relevant code:** Look in `src/main/java/diesel/parser/` or `src/main/java/diesel/sql/` directories.

### 4. Add JMH benchmark for SELECT WHERE on indexed column
**Issue:** Create a performance benchmark for SELECT queries with indexed columns  
**Complexity:** Intermediate  
**Expected time:** 2 hours  
**Context:** Performance is important for database systems. We need benchmarks to track performance improvements and regressions.  
**What to do:**
- Set up JMH (Java Microbenchmark Harness) in the project
- Create a benchmark class for SELECT WHERE queries on indexed columns
- Test different table sizes (1K, 10K, 100K rows)
- Measure query execution time
- Add the benchmark to the performance test suite
- Document the benchmark setup and results

**Link to relevant code:** Look at existing performance tests and add a new benchmark in `src/test/java/diesel/performance/`.

### 5. Translate README sections to English (currently mixed RU/EN)
**Issue:** Translate Russian sections of README.md to English  
**Complexity:** Beginner  
**Expected time:** 1-2 hours  
**Context:** The README has mixed Russian and English content, which makes it harder for international users to understand.  
**What to do:**
- Identify all Russian sections in README.md
- Translate them to clear, professional English
- Maintain consistency with existing English content
- Ensure technical terms are translated correctly
- Update any references to Russian documentation
- Proofread for clarity and accuracy

**Link to relevant code:** Read through README.md and identify sections that need translation.

## 🚀 How to Get Started

1. **Fork the repository** and create a feature branch
2. **Check out the CONTRIBUTING.md** for setup instructions
3. **Pick an issue** that interests you
4. **Start with a small change** and test it thoroughly
5. **Submit a pull request** with clear description
6. **Ask for help** if you get stuck

## 💡 Tips for Success

- Start with issues labeled "good first issue" - they're designed for newcomers
- Read the existing code to understand the patterns and conventions
- Run tests before and after your changes
- Ask questions in Discussions if you need help
- Don't worry about making mistakes - that's how we all learn!

## 📚 Resources

- [Contributing Guide](CONTRIBUTING.md)
- [Discussions](https://github.com/Reider85/dieseldb/discussions)
- [Existing Issues](https://github.com/Reider85/dieseldb/issues)
- [Code Style Guidelines](CONTRIBUTING.md#code-style)

Good luck and happy coding! 🎉