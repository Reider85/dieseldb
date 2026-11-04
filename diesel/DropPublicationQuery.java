package diesel;

import diesel.replication.Publication;
import diesel.replication.ReplicationRegistry;

/**
 * Executes a DROP PUBLICATION statement: drops a logical replication publication.
 * 
 * <p>Example: DROP PUBLICATION my_pub
 */
public class DropPublicationQuery implements Query<Integer> {
    
    private final String publicationName;
    
    public DropPublicationQuery(String publicationName) {
        this.publicationName = publicationName;
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
        
        // Check if publication exists
        Publication publication = registry.getPublication(publicationName);
        if (publication == null) {
            throw new IllegalStateException("Publication '" + publicationName + "' does not exist");
        }
        
        // Drop the publication (this will also drop all associated subscriptions)
        registry.dropPublication(publicationName);
        
        return 1; // Number of publications dropped
    }
    
    public String getPublicationName() {
        return publicationName;
    }
}