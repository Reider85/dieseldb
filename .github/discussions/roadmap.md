# Roadmap v3.5 / v4.0 — what's planned

This discussion outlines the development roadmap for DieselDB as we work toward v1.0.0 stable release. Your feedback and suggestions are valuable for shaping the future of this project!

## 🎯 Current Status (v3.3.99)

DieselDB is currently in **production-like** state with core database engine features implemented and actively tested. The project follows a prompt-driven development approach with regular releases.

### ✅ Production-Implemented
- Complete SQL DDL/DML/DQL support
- ARIES crash recovery system
- MVCC with SSI isolation
- Online schema changes
- Multiple storage backends
- PostgreSQL-compatible protocol
- Security features (RBAC, TLS)
- Replication and backup systems
- Comprehensive observability

### 🚧 In Active Development
The following features are currently being implemented:

1. **Logical replication with auto-failover** (Patroni-equivalent)
2. **Constraint system** (FOREIGN KEY, CHECK, NOT NULL, DEFAULT)
3. **JSON column type** (binary JSONB-like storage)
4. **Advanced SQL features** (CTE, UNION, INTERSECT, EXCEPT)
5. **Window functions** (OVER, PARTITION BY, ROW_NUMBER, RANK)

## 📋 v3.5 Development Goals

### Phase 1: Core Enhancements (Q1 2027)
- **Logical replication completion**
  - Publication/Subscription system
  - Conflict resolution strategies
  - Auto-failover with leader election
  - Integration with existing physical replication

- **Constraint system implementation**
  - FOREIGN KEY with cascading operations
  - CHECK constraint validation
  - NOT NULL and DEFAULT constraints
  - Constraint violation handling

### Phase 2: Advanced Features (Q2 2027)
- **JSON data type**
  - Binary storage format (JSONB-like)
  - JSON path queries and functions
  - Indexing support for JSON fields
  - Integration with existing storage engines

- **Window functions**
  - OVER clause support
  - PARTITION BY, ORDER BY
  - ROW_NUMBER, RANK, DENSE_RANK, NTILE
  - Frame specifications (ROWS/RANGE)

### Phase 3: Performance & Optimization (Q3 2027)
- **Query optimization improvements**
  - Cost-based optimizer enhancements
  - Better index selection algorithms
  - Parallel query execution
  - Adaptive execution plans

- **Performance monitoring**
  - Enhanced query profiling
  - Performance regression detection
  - Resource usage tracking
  - Automated performance tuning

## 🚀 v4.0 Vision (Late 2027)

### Enterprise Features
- **High availability** with automatic failover
- **Distributed database** capabilities
- **Cloud-native** deployment options
- **Container orchestration** (Kubernetes operators)

### Developer Experience
- **GUI administration tool**
- **Visual query builder**
- **Performance dashboard**
- **IDE plugins** for development

### Ecosystem Expansion
- **Additional drivers** (C#, Ruby, PHP)
- **ORM integrations** (more than current 5 drivers)
- **Migration tools** for popular databases
- **Benchmarking suite** for performance testing

## 🔄 Development Methodology

### Prompt-Driven Development
DieselDB uses a unique AI-assisted, prompt-driven development approach:
- **Regular releases** (multiple per week)
- **Continuous integration** with automated testing
- **Feature-focused commits** with clear scope
- **Comprehensive documentation** updates

### Testing Strategy
- **368 test classes** covering all subsystems
- **Multiple storage backends** testing
- **Concurrency and isolation** testing
- **Performance regression** testing
- **Crash recovery** validation

### Quality Assurance
- **Code coverage** monitoring
- **Static analysis** integration
- **Performance benchmarks**
- **Security scanning**

## 💡 Community Input Wanted

We value your feedback on the roadmap! Please share your thoughts on:

### Feature Priorities
Which features are most important to you?
- Logical replication
- Constraint system
- JSON support
- Window functions
- Performance optimization
- Other features?

### Use Cases
How are you planning to use DieselDB?
- Embedded database for applications
- Standalone database server
- Development/testing environment
- Educational purposes
- Research projects

### Integration Needs
What integrations would be most valuable?
- Specific frameworks (Spring, Hibernate, etc.)
- Cloud platforms
- Monitoring tools
- Development tools
- Other databases

### Performance Requirements
What performance characteristics matter most to you?
- Query speed
- Concurrency handling
- Memory usage
- Disk I/O optimization
- Network performance

## 🎯 Success Metrics

We measure success by:
- **User adoption** and community growth
- **Code quality** and test coverage
- **Performance benchmarks** and improvements
- **Feature completeness** vs. production databases
- **Documentation quality** and accessibility

## 📅 Timeline

### Q1 2027
- Complete logical replication
- Implement constraint system
- Release v3.5.0

### Q2 2027
- JSON data type implementation
- Window functions
- Performance optimizations

### Q3 2027
- Advanced query optimization
- Enhanced monitoring
- Release v3.6.0

### Q4 2027
- v4.0 planning and preparation
- Enterprise features design
- Community feedback integration

## 🤝 How to Contribute

### Code Contributions
- Fork the repository
- Pick features from the roadmap
- Submit pull requests
- Help with testing and documentation

### Feedback and Suggestions
- Participate in this discussion
- Create feature requests
- Share your use cases
- Report issues and bugs

### Community Building
- Share your DieselDB experiences
- Create tutorials and examples
- Help other users in discussions
- Promote the project

## 🙏 Thank You

Thank you for your interest in DieselDB and for helping shape its future! Your participation in this roadmap discussion helps us build a database that meets real-world needs.

**Remember**: ⭐ Star this repo if you find DieselDB useful — it helps others discover it!

---

*What features are you most excited about? Share your thoughts below!*