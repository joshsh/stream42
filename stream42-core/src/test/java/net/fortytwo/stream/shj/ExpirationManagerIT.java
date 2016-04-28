package net.fortytwo.stream.shj;

import org.junit.Test;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class ExpirationManagerIT extends SHJTestBase {

    /*
    On test machine Marvin5, between 1200 and 2400 items/ms are evicted from a heap with as many as 1M items.
    There is the expected logarithmic decline in eviction rate with increasing heap size.
    Eviction time from a small heap is thus negligible in comparison with RDF indexing and removal time.

    ##########
    # Bash
    echo "" > /tmp/in.txt && vim /tmp/in.txt && cat /tmp/in.txt | grep evicted| sed 's/.*evicted //' | sed 's/ of /,/' | sed 's/.items in./,/'|sed 's/.ms//' > /tmp/eviction-time.csv

    ##########
    # R

    data <- read.csv(file("/tmp/eviction-time.csv"), header=FALSE)
    count <- data$V1
    size <- data$V2
    time <- data$V3

    rate <- count/time
    plot(x=size, y=rate, type = "n", log="x", xlab="heap size", ylab="# evicted per millisecond")
    points(x=size, y=rate)
    m <- mean(rate)
    d <- sd(rate)
    abline(h=m, col="red")
    abline(h=(m+d), lty="dashed", col="red")
    abline(h=(m-d), lty="dashed", col="red")
    */
    @Test
    public void testEvictionRate() throws Exception {
        long span = 1000L;
        long maxTtl = span * 10;

        for (int i = 1; i <= 10; i++) {
            solutionExpirationManager.clear();

            // build an expiration heap of variable size, with TTLs uniformly distributed
            int total = i * 100000;
            for (int j = 0; j < total; j++) {
                long ttl = 1L + (long) (random.nextDouble() * maxTtl);
                solutionExpirationManager.add(randomSolution(4, ttl));
            }

            // evict an approximately constant fraction of items
            int size = solutionExpirationManager.getHeapSize();
            long before = System.currentTimeMillis();
            setCurrentTime(span);
            int evictCount = solutionExpirationManager.evictExpired();
            long after = System.currentTimeMillis();
            if (evictCount > 0) {
                System.out.println("###\t\tevicted " + evictCount + " of " + size + " items in " + (after - before) + " ms");
            }
        }
    }

    @Test
    public void testEvictionThread() {
        solutionExpirationManager.start();
        try {
            long now;
            long stopAt = System.currentTimeMillis() + 10000L;
            long maxTtl = 10000L;
            while ((now = setCurrentTime(System.currentTimeMillis())) < stopAt) {
                solutionExpirationManager.evictExpired();
                long ttl = (long) (random.nextDouble() * maxTtl);
                Solution<String> d = randomSolution(4, now + ttl);
                solutionExpirationManager.add(d);
                solutionExpirationManager.notifyFinishedAdding();
            }
        } finally {
            solutionExpirationManager.stop();
        }
    }

    /**
     * Tests automatic eviction without the additional, explicit eviction call before the addition of each datum.
     */
    @Test
    public void testEvictionThreadWithoutExplicitCall() {
        solutionExpirationManager.start();

        long now;
        long stopAt = System.currentTimeMillis() + 10000L;
        long maxTtl = 10000L;
        while ((now = setCurrentTime(System.currentTimeMillis())) < stopAt) {
            long ttl = (long) (random.nextDouble() * maxTtl);
            Solution<String> d = randomSolution(4, now + ttl);
            solutionExpirationManager.add(d);
            solutionExpirationManager.notifyFinishedAdding();
        }
    }
}
