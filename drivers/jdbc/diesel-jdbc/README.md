# DieselDB JDBC Driver

A JDBC driver for connecting to DieselDB libpq server, compatible with JDBC 4.2 and Hibernate 6.

## Features

- Full JDBC 4.2 compliance
- Support for basic CRUD operations
- Prepared statements and batch updates
- Transaction support with savepoints
- Hibernate 6 compatibility
- Zero runtime dependencies

## Installation

### Maven

```xml
<dependency>
    <groupId>com.dieseldb</groupId>
    <artifactId>diesel-jdbc</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.dieseldb:diesel-jdbc:0.1.0'
```

## Usage

### Basic Connection

```java
import java.sql.*;

public class DieselDBExample {
    public static void main(String[] args) {
        String url = "jdbc:diesel:localhost:5432/your_database";
        String user = "username";
        String password = "password";
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to DieselDB!");
            
            // Create statement
            try (Statement stmt = conn.createStatement()) {
                // Execute query
                ResultSet rs = stmt.executeQuery("SELECT version()");
                if (rs.next()) {
                    System.out.println("Database version: " + rs.getString(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### Prepared Statements

```java
try (Connection conn = DriverManager.getConnection(url, user, password);
     PreparedStatement pstmt = conn.prepareStatement(
         "INSERT INTO users (id, name, email) VALUES (?, ?, ?)")) {
    
    pstmt.setInt(1, 1);
    pstmt.setString(2, "Alice");
    pstmt.setString(3, "alice@example.com");
    pstmt.executeUpdate();
    
    // Batch updates
    pstmt.setInt(1, 2);
    pstmt.setString(2, "Bob");
    pstmt.setString(3, "bob@example.com");
    pstmt.addBatch();
    
    pstmt.setInt(1, 3);
    pstmt.setString(2, "Charlie");
    pstmt.setString(3, "charlie@example.com");
    pstmt.addBatch();
    
    int[] results = pstmt.executeBatch();
    System.out.println("Inserted " + results.length + " rows");
}
```

### Transactions

```java
try (Connection conn = DriverManager.getConnection(url, user, password)) {
    conn.setAutoCommit(false);
    
    try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
        stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (2, 500)");
        conn.commit();
    } catch (SQLException e) {
        conn.rollback();
        throw e;
    }
    
    conn.setAutoCommit(true);
}
```

### Database Metadata

```java
try (Connection conn = DriverManager.getConnection(url, user, password)) {
    DatabaseMetaData metaData = conn.getMetaData();
    
    System.out.println("Database: " + metaData.getDatabaseProductName());
    System.out.println("Version: " + metaData.getDatabaseProductVersion());
    System.out.println("Driver: " + metaData.getDriverName());
    
    // Get tables
    try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
        while (tables.next()) {
            System.out.println("Table: " + tables.getString("TABLE_NAME"));
        }
    }
}
```

## Hibernate Integration

### Configuration

```xml
<!-- persistence.xml -->
<persistence version="3.0" xmlns="https://jakarta.ee/xml/ns/persistence">
    <persistence-unit name="dieselPU">
        <properties>
            <property name="jakarta.persistence.jdbc.driver" 
                      value="com.diesel.jdbc.Driver"/>
            <property name="jakarta.persistence.jdbc.url" 
                      value="jdbc:diesel:localhost:5432/your_database"/>
            <property name="jakarta.persistence.jdbc.user" 
                      value="username"/>
            <property name="jakarta.persistence.jdbc.password" 
                      value="password"/>
            <property name="hibernate.dialect" 
                      value="org.hibernate.dialect.DieselDBDialect"/>
            <property name="hibernate.show_sql" 
                      value="true"/>
            <property name="hibernate.format_sql" 
                      value="true"/>
            <property name="hibernate.hbm2ddl.auto" 
                      value="update"/>
        </properties>
    </persistence-unit>
</persistence>
```

### Entity Example

```java
import jakarta.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, length = 100)
    private String name;
    
    @Column(length = 100)
    private String email;
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
```

## Testing

Run the test suite:

```bash
# Run all tests
mvn test

# Run only integration tests
mvn test -P integration-test

# Run only Hibernate compatibility tests
mvn test -P hibernate-test

# Run tests with local DieselDB engine
mvn test -P local-test
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/Reider85/dieseldb.git
cd dieseldb/drivers/jdbc/diesel-jdbc

# Build the project
mvn clean compile

# Run tests
mvn test

# Build JAR with sources and Javadoc
mvn package
```

## Deployment to Maven Central

### Prerequisites

1. Generate a GPG key
2. Create account at Sonatype OSSRH
3. Create JIRA ticket for project

### Steps

1. **Configure your settings.xml**:

```xml
<settings>
    <servers>
        <server>
            <id>ossrh</id>
            <username>your-sonatype-username</username>
            <password>your-sonatype-password</password>
        </server>
    </servers>
</settings>
```

2. **Set GPG passphrase** (optional, for automated signing):

```bash
export GPG_PASSPHRASE="your-passphrase"
export GPG_KEYNAME="your-key-id"
export STAGING_PROFILE_ID="your-staging-profile-id"
```

3. **Deploy to staging**:

```bash
mvn clean deploy -P release
```

4. **Release to Maven Central**:
   - Go to https://s01.oss.sonatype.org
   - Find your staged artifacts
   - Promote the repository to "Release"

### Manual Deployment

```bash
# Build all artifacts
mvn clean package

# Sign artifacts
mvn gpg:sign -P release

# Deploy to staging
mvn deploy -P release
```

## Development

### Project Structure

```
diesel-jdbc/
├── src/main/java/com/diesel/jdbc/
│   ├── Driver.java              # JDBC driver registration
│   ├── DieselConnection.java     # Connection implementation
│   ├── DieselStatement.java      # Statement implementation
│   ├── DieselPreparedStatement.java # Prepared statement
│   ├── DieselResultSet.java      # Result set implementation
│   └── DieselDatabaseMetaData.java # Database metadata
├── src/test/java/com/diesel/jdbc/
│   ├── DieselJdbcIntegrationTest.java  # Integration tests
│   └── DieselHibernateIntegrationTest.java # Hibernate tests
├── pom.xml                      # Maven configuration
└── README.md                    # This file
```

### Adding New Features

1. Add tests for new functionality
2. Ensure backward compatibility
3. Update documentation
4. Test with Hibernate if applicable

### Code Style

- Follow Java conventions
- Use proper exception handling
- Add meaningful comments
- Ensure thread safety where needed

## Troubleshooting

### Common Issues

1. **ClassNotFoundException**: Ensure the driver is in your classpath
2. **Connection refused**: Check if DieselDB server is running
3. **Authentication failed**: Verify username and password
4. **SQL syntax errors**: Check your SQL against DieselDB dialect

### Debug Mode

Enable debug logging:

```java
java -Djava.util.logging.config=logging.properties -jar your-app.jar
```

### Support

- GitHub Issues: https://github.com/Reider85/dieseldb/issues
- Documentation: https://dieseldb.readthedocs.io/

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Version History

- 0.1.0 - Initial release
  - Basic JDBC 4.2 compliance
  - Hibernate 6 compatibility
  - Maven Central deployment pipeline