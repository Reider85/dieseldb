package diesel;

import diesel.replication.Publication;
import diesel.replication.ReplicationRegistry;
import java.util.Set;

/**
 * Executes a CREATE PUBLICATION statement: creates a logical replication publication.
 * 
 * <p>Example: CREATE PUBLICATION my_pub FOR TABLE users, orders WITH (conflict_resolution = skip)
 */
public class CreatePublicationQuery implements Query<Integer> {
    
    private final Publication publication;
    
    public CreatePublicationQuery(Publication publication) {
        this.publication = publication;
    }
    
    @Override
    public Integer execute(Table table) {
        // Get the database from the table
        Database database = table.getDatabase();
        
        // Get the replication registry from the database
        ReplicationRegistry registry = database.getLogicalReplicationRegistry();
        if (registry == null) {
            throw new IllegalStateException("Replication registry not available");
        }
        
        // Check if publication already exists
        if (registry.getPublication(publication.getName()) != null) {
            throw new IllegalStateException("Publication '" + publication.getName() + "' already exists");
        }
        
        // Create the publication
        registry.createPublication(publication);
        
        return 1; // Number of publications created
    }
    
    public Publication getPublication() {
        return publication;
    }
}