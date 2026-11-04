package diesel;

import diesel.replication.Subscription;
import diesel.replication.ReplicationRegistry;

/**
 * Executes a DROP SUBSCRIPTION statement: drops a logical replication subscription.
 * 
 * <p>Example: DROP SUBSCRIPTION my_sub
 */
public class DropSubscriptionQuery implements Query<Integer> {
    
    private final String subscriptionName;
    
    public DropSubscriptionQuery(String subscriptionName) {
        this.subscriptionName = subscriptionName;
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
        
        // Check if subscription exists
        Subscription subscription = registry.getSubscription(subscriptionName);
        if (subscription == null) {
            throw new IllegalStateException("Subscription '" + subscriptionName + "' does not exist");
        }
        
        // Drop the subscription
        registry.dropSubscription(subscriptionName);
        
        return 1; // Number of subscriptions dropped
    }
    
    public String getSubscriptionName() {
        return subscriptionName;
    }
}