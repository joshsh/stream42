<!-- This README can be viewed at https://github.com/joshsh/sesamestream/wiki -->

## SesameStream

![SesameStream logo|width=94px|height=65px](https://github.com/joshsh/sesamestream/wiki/graphics/sesamestream-logo-small.png)

SesameStream is a continuous SPARQL query engine for real-time applications, built with the [Sesame](http://www.openrdf.org/) RDF framework.  It implements a subset (see below) of the [SPARQL](http://www.w3.org/TR/sparql11-query/) query language and matches streaming RDF data against queries with low latency, responding to individual statements with the query answers they complete.  The query engine conserves resources by indexing only those statements which match against already-submitted queries, making it appropriate for processing long, noisy data streams.  SesameStream was developed for use in [wearable and ubiquitous computing](https://github.com/joshsh/extendo) contexts in which sensor data must be combined with background semantics under real time constraints of a few tens of milliseconds.

Here is a usage example in Java:

```java
String query = "SELECT ?foo ?bar WHERE { ... }";

QueryEngine queryEngine = new QueryEngineImpl();

BindingSetHandler handler = new BindingSetHandler() {
    public void handle(final BindingSet answer) {
        System.out.println("found an answer to the continuous query: " + answer);
    }
};
Subscription sub = queryEngine.addQuery(query, handler);    

// normally, this would be a streaming data source
Collection<Statement> data = ...
for (Statement s : data) {
    // as new statements are added, computed query answers will be pushed to the BindingSetHandler
    queryEngine.addStatement(s);        
}

// cancel the query subscription at any time;
// no further answers will be computed/produced for the corresponding query
sub.cancel();
```

See also:
* [SesameStream API](http://fortytwo.net/projects/sesamestream/api/latest/)

Send questions or comments to:

![Josh email](http://fortytwo.net/Home_files/josh_email.jpg)


### Syntax reference

SPARQL syntax currently supported by SesameStream includes:
* [SELECT](http://www.w3.org/TR/sparql11-query/#select) queries.  SELECT subscriptions in SesameStream produce query answers indefinitely unless cancelled.
* [ASK](http://www.w3.org/TR/sparql11-query/#ask) queries.  ASK subscriptions produce at most one query answer (indicating a result of **true**) and then are cancelled automatically, similarly to a SELECT query with a LIMIT of 1.
* [CONSTRUCT](http://www.w3.org/TR/sparql11-query/#construct) queries.  Each query answer contains "subject", "predicate", and "object" bindings which may be turned into an RDF statement.
* [basic graph patterns](http://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
* [variable projection](http://www.w3.org/TR/sparql11-query/#modProjection)
* all [RDF Term syntax](http://www.w3.org/TR/sparql11-query/#syntaxTerms) and [triple pattern](http://www.w3.org/TR/sparql11-query/#QSynTriples) syntax via Sesame
* [FILTER](http://www.w3.org/TR/sparql11-query/#tests) constraints, with all SPARQL [operator functions](http://www.w3.org/TR/sparql11-query/#SparqlOps) supported via Sesame **except for** [EXISTS](http://www.w3.org/TR/sparql11-query/#func-filter-exists)
* [DISTINCT](http://www.w3.org/TR/sparql11-query/#modDuplicates) modifier.  Use with care if the streaming data source may produce an unlimited number of solutions.
* [REDUCED](http://www.w3.org/TR/sparql11-query/#modDuplicates) modifier.  Similar to DISTINCT, but safe for long streams.  Each subscription maintains a solution set which begins to recycle after it reaches a certain size, configurable with `SesameStream.setReducedModifierCapacity()`.
* [LIMIT](http://www.w3.org/TR/sparql11-query/#modResultLimit) clause.  Once LIMIT number of answers have been produced, the subscription is cancelled.
* [OFFSET](http://www.w3.org/TR/sparql11-query/#modOffset) clause.  Since query answers roughly follow the order in which input statements are received, OFFSET can be practically useful even without ORDER BY (see below)

Syntax explicitly not supported:
* [ORDER BY](http://www.w3.org/TR/sparql11-query/#modOrderBy).  This is a closed-world operation which requires a finite data set or window; SesameStream queries over a stream of data and an infinite window.
* SPARQL 1.1 [aggregates](http://www.w3.org/TR/sparql11-query/#aggregates).  See above

Syntax not yet supported:
* [DESCRIBE](http://www.w3.org/TR/sparql11-query/#describe) query form
* [OPTIONAL](http://www.w3.org/TR/sparql11-query/#optionals) and [UNION](http://www.w3.org/TR/sparql11-query/#alternatives) patterns, [group graph patterns](http://www.w3.org/TR/sparql11-query/#GroupPatterns)
* [RDF Dataset syntax](http://www.w3.org/TR/sparql11-query/#rdfDataset), i.e. the FROM, FROM NAMED, and GRAPH keywords
* SPARQL 1.1's [NOT](http://www.w3.org/TR/sparql11-query/#negation), [Property Paths](http://www.w3.org/TR/sparql11-query/#propertypaths), [assignment](http://www.w3.org/TR/sparql11-query/#assignment) (BIND / AS / VALUES), [subqueries](http://www.w3.org/TR/sparql11-query/#subqueries)
* SPARQL 1.1 [Federated Query](http://www.w3.org/TR/sparql11-federated-query/) syntax
