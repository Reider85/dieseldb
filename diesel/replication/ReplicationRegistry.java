package diesel.replication;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Registry for managing logical replication publications and subscriptions.
 * Provides thread-safe access to replication metadata.
 */
public class ReplicationRegistry {
    
    private final Map<String, Publication> publications = new ConcurrentHashMap<>();
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    
    /**
     * Creates a new publication.
     * 
     * @param publication The publication to create
     * @throws IllegalStateException if publication already exists
     */
    public void createPublication(Publication publication) {
        if (publications.containsKey(publication.getName())) {
            throw new IllegalStateException("Publication '" + publication.getName() + "' already exists");
        }
        publications.put(publication.getName(), publication);
    }
    
    /**
     * Drops a publication and all its associated subscriptions.
     * 
     * @param publicationName Name of the publication to drop
     * @throws IllegalStateException if publication does not exist
     */
    public void dropPublication(String publicationName) {
        Publication publication = publications.remove(publicationName);
        if (publication == null) {
            throw new IllegalStateException("Publication '" + publicationName + "' does not exist");
        }
        
        // Drop all subscriptions associated with this publication
        subscriptions.entrySet().removeIf(entry -> 
            entry.getValue().getPublicationName().equals(publicationName));
    }
    
    /**
     * Gets a publication by name.
     * 
     * @param publicationName Name of the publication
     * @return The publication, or null if not found
     */
    public Publication getPublication(String publicationName) {
        return publications.get(publicationName);
    }
    
    /**
     * Gets all publications.
     * 
     * @return Set of all publications
     */
    public Set<Publication> getAllPublications() {
        return new CopyOnWriteArraySet<>(publications.values());
    }
    
    /**
     * Checks if a publication exists.
     * 
     * @param publicationName Name of the publication
     * @return true if publication exists, false otherwise
     */
    public boolean publicationExists(String publicationName) {
        return publications.containsKey(publicationName);
    }
    
    /**
     * Creates a new subscription.
     * 
     * @param subscription The subscription to create
     * @throws IllegalStateException if subscription already exists
     */
    public void createSubscription(Subscription subscription) {
        if (subscriptions.containsKey(subscription.getName())) {
            throw new IllegalStateException("Subscription '" + subscription.getName() + "' already exists");
        }
        
        // Verify that the publication exists
        Publication publication = publications.get(subscription.getPublicationName());
        if (publication == null) {
            throw new IllegalStateException("Publication '" + subscription.getPublicationName() + "' does not exist");
        }
        
        subscriptions.put(subscription.getName(), subscription);
    }
    
    /**
     * Drops a subscription.
     * 
     * @param subscriptionName Name of the subscription to drop
     * @throws IllegalStateException if subscription does not exist
     */
    public void dropSubscription(String subscriptionName) {
        Subscription subscription = subscriptions.remove(subscriptionName);
        if (subscription == null) {
            throw new IllegalStateException("Subscription '" + subscriptionName + "' does not exist");
        }
    }
    
    /**
     * Gets a subscription by name.
     * 
     * @param subscriptionName Name of the subscription
     * @return The subscription, or null if not found
     */
    public Subscription getSubscription(String subscriptionName) {
        return subscriptions.get(subscriptionName);
    }
    
    /**
     * Gets all subscriptions.
     * 
     * @return Set of all subscriptions
     */
    public Set<Subscription> getAllSubscriptions() {
        return new CopyOnWriteArraySet<>(subscriptions.values());
    }
    
    /**
     * Gets all subscriptions for a specific publication.
     * 
     * @param publicationName Name of the publication
     * @return Set of subscriptions for the publication
     */
    public Set<Subscription> getSubscriptionsForPublication(String publicationName) {
        Set<Subscription> result = new CopyOnWriteArraySet<>();
        for (Subscription subscription : subscriptions.values()) {
            if (subscription.getPublicationName().equals(publicationName)) {
                result.add(subscription);
            }
        }
        return result;
    }
    
    /**
     * Checks if a subscription exists.
     * 
     * @param subscriptionName Name of the subscription
     * @return true if subscription exists, false otherwise
     */
    public boolean subscriptionExists(String subscriptionName) {
        return subscriptions.containsKey(subscriptionName);
    }
    
    /**
     * Gets all publications that include the specified table.
     * 
     * @param tableName Name of the table
     * @return Set of publications that include the table
     */
    public Set<Publication> getPublicationsForTable(String tableName) {
        Set<Publication> result = new CopyOnWriteArraySet<>();
        for (Publication publication : publications.values()) {
            if (publication.containsTable(tableName)) {
                result.add(publication);
            }
        }
        return result;
    }
    
    /**
     * Clears all publications and subscriptions.
     */
    public void clear() {
        publications.clear();
        subscriptions.clear();
    }
    
    /**
     * Gets the total number of publications.
     * 
     * @return Number of publications
     */
    public int getPublicationCount() {
        return publications.size();
    }
    
    /**
     * Gets the total number of subscriptions.
     * 
     * @return Number of subscriptions
     */
    public int getSubscriptionCount() {
        return subscriptions.size();
    }
}