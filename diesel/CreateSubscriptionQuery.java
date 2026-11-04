package diesel;

import diesel.replication.Subscription;
import diesel.replication.ReplicationRegistry;
import diesel.replication.Publication;
import java.util.UUID;

/**
 * Executes a CREATE SUBSCRIPTION statement: creates a logical replication subscription.
 * 
 * <p>Example: CREATE SUBSCRIPTION my_sub TO PUBLICATION my_pub CONNECTION 'localhost:5432' WITH (conflict_resolution = replace)
 */
public class CreateSubscriptionQuery implements Query<Integer> {
    
    private final String name;
    private final String publicationName;
    private final String masterHost;
    private final int masterPort;
    private final Subscription.ConflictResolutionStrategy conflictResolution;
    
    public CreateSubscriptionQuery(String name, String publicationName, String masterHost, int masterPort,
                                  Subscription.ConflictResolutionStrategy conflictResolution) {
        this.name = name;
        this.publicationName = publicationName;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.conflictResolution = conflictResolution;
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
        
        // Check if subscription already exists
        if (registry.getSubscription(name) != null) {
            throw new IllegalStateException("Subscription '" + name + "' already exists");
        }
        
        // Create the subscription
        Subscription subscription = new Subscription(name, publicationName, masterHost, masterPort, 
                                                 conflictResolution, true);
        registry.createSubscription(subscription);
        
        return 1; // Number of subscriptions created
    }
    
    public String getName() {
        return name;
    }
    
    public String getPublicationName() {
        return publicationName;
    }
    
    public String getMasterHost() {
        return masterHost;
    }
    
    public int getMasterPort() {
        return masterPort;
    }
    
    public Subscription.ConflictResolutionStrategy getConflictResolution() {
        return conflictResolution;
    }
}