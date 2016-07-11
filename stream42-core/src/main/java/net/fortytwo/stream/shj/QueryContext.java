package net.fortytwo.stream.shj;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryContext<K, V> {

    private final ExpirationManager<Solution<V>> solutionExpirationManager;
    private final ExpirationManager<Query<K, V>> queryExpirationManager;

    public QueryContext(ExpirationManager<Query<K, V>> queryExpirationManager,
                        ExpirationManager<Solution<V>> solutionExpirationManager) {
        this.queryExpirationManager = queryExpirationManager;
        this.solutionExpirationManager = solutionExpirationManager;
    }

    public static <T> Set<T> newConcurrentSet() {
        return Collections.newSetFromMap(new HashMap<>());
    }

    public void evictExpired() {
        queryExpirationManager.evictExpired();
        solutionExpirationManager.evictExpired();
    }

    public ExpirationManager<Solution<V>> getSolutionExpirationManager() {
        return solutionExpirationManager;
    }

    public ExpirationManager<Query<K, V>> getQueryExpirationManager() {
        return queryExpirationManager;
    }

    public void clear() {
        solutionExpirationManager.clear();
        queryExpirationManager.clear();
    }
}
