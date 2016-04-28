package net.fortytwo.stream;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * An object which associates a query with a handler for the query's results
 *
 * @param <Q> the native query type
 * @param <C> the constraint type
 * @param <S> the solution type
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class BasicSubscription<C, Q, S> implements Subscription {
    private static int nextId = 0;

    private final C sparqlQuery;
    private Q query;
    private BiConsumer<S, Long> solutionConsumer;
    private final BasicStreamProcessor<?, C, Q, ?, S> processor;

    private boolean active;

    private final String id;

    public synchronized static String getNextId() {
        return Integer.valueOf(nextId++).toString();
    }

    public BasicSubscription(final String id,
                             final C sparqlQuery,
                             final Q query,
                             final BiConsumer<S, Long> consumer,
                             final BasicStreamProcessor<?, C, Q, ?, S> processor) {
        this.id = id;
        this.sparqlQuery = sparqlQuery;
        this.query = query;
        this.solutionConsumer = consumer;
        this.processor = processor;

        this.active = true;
    }

    public BasicSubscription(final C sparqlQuery,
                             final Q query,
                             final BiConsumer<S, Long> solutionConsumer,
                             final BasicStreamProcessor<?, C, Q, ?, S> processor) {
        this(getNextId(), sparqlQuery, query, solutionConsumer, processor);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void cancel() throws IOException {
        active = false;

        processor.unregister(this);
    }

    @Override
    public boolean renew(int ttl) throws IOException {
        return isActive() && processor.renew(this, ttl);
    }

    /**
     * @return the SPARQL query which has been registered in the {@link net.fortytwo.stream.StreamProcessor}
     */
    public C getConstraint() {
        return sparqlQuery;
    }

    /**
     * @return the graph pattern which has been indexed in the forward-chaining tuple store implementation
     */
    public Q getQuery() {
        return query;
    }

    /**
     * @return the handler for the query's results
     */
    public BiConsumer<S, Long> getSolutionConsumer() {
        return solutionConsumer;
    }

    public void setQuery(Q query) {
        this.query = query;
    }

    public void setSolutionConsumer(BiConsumer<S, Long> solutionConsumer) {
        this.solutionConsumer = solutionConsumer;
    }
}
