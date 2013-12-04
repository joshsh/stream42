<!-- This README can be viewed at https://github.com/joshsh/sesamestream/wiki -->

![SesameStream logo|width=94px|height=65px](https://github.com/joshsh/sesamestream/wiki/graphics/sesamestream-logo-small.png)

SesameStream is a continuous SPARQL query engine for real-time applications, built with the [Sesame](http://www.openrdf.org/) RDF framework.  It implements a basic subset of the [SPARQL](http://www.w3.org/TR/sparql11-query/) query language and matches streaming RDF data against these queries with very low latency, responding to individual statements with the query answers they complete.  The query engine conserves resources by indexing only those statements which match against already-submitted queries, making it appropriate for processing long, noisy data streams.  SesameStream was developed for use in [wearable and ubiquitous computing](https://github.com/joshsh/extendo) contexts in which sensor data must be combined with background semantics under real time constraints of a few tens of milliseconds.

Here is a usage example in Java:

```java
String queryStr = "SELECT ?foo ?bar WHERE { ... }";
    
ParsedQuery query = QueryParserUtil.parseQuery(
        QueryLanguage.SPARQL,
        queryStr,
        "http://example.org/baseURI");

queryEngine = new QueryEngine();
queryEngine.addQuery(query.getTupleExpr(), new BindingSetHandler() {
    public void handle(final BindingSet bindings) {
        LOGGER.info("found a result for the continuous query");
        // do something useful with the BindingSet (query result)
    }
});    
    
// normally, this would be a streaming data source
Collection<Statement> data = ...
for (Statement s : data) {
    // as new statements are added, query results will be pushed to the BindingSetHandler
    queryEngine.addStatement(s);        
}
```

See also:
* [SesameStream API](http://fortytwo.net/projects/sesamestream/api/latest/)

Send questions or comments to:

![Josh email](http://fortytwo.net/Home_files/josh_email.jpg)


### Syntax reference

SPARQL syntax currently supported by SesameStream includes:
* [SELECT](http://www.w3.org/TR/sparql11-query/#select) queries for [basic graph patterns](http://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
* [variable projection](http://www.w3.org/TR/sparql11-query/#modProjection)
* all [RDF Term syntax](http://www.w3.org/TR/sparql11-query/#syntaxTerms) and [triple pattern](http://www.w3.org/TR/sparql11-query/#QSynTriples) syntax via Sesame
* [FILTER](http://www.w3.org/TR/sparql11-query/#tests) constraints, with all SPARQL [operator function](http://www.w3.org/TR/sparql11-query/#SparqlOps) implementations inherited from Sesame
* [DISTINCT](http://www.w3.org/TR/sparql11-query/#modDuplicates) modifier.  Use with care if the streaming data source may produce an unlimited number of solutions

Syntax explicitly not supported:
* [ORDER BY](http://www.w3.org/TR/sparql11-query/#modOrderBy).  This is a closed-world operation which requires a finite data set or window; SesameStream queries over a stream of data and an infinite window.
* SPARQL 1.1 [aggregates](http://www.w3.org/TR/sparql11-query/#aggregates).  See above

Syntax not yet supported:
* [CONSTRUCT](http://www.w3.org/TR/sparql11-query/#construct), [ASK](http://www.w3.org/TR/sparql11-query/#ask), and [DESCRIBE](http://www.w3.org/TR/sparql11-query/#describe) query forms
* [OPTIONAL](http://www.w3.org/TR/sparql11-query/#optionals) and [UNION](http://www.w3.org/TR/sparql11-query/#alternatives) patterns, [group graph patterns](http://www.w3.org/TR/sparql11-query/#GroupPatterns)
* [RDF Dataset syntax](http://www.w3.org/TR/sparql11-query/#rdfDataset), i.e. the FROM, FROM NAMED, and GRAPH keywords
* [LIMIT](http://www.w3.org/TR/sparql11-query/#modResultLimit) and [OFFSET](http://www.w3.org/TR/sparql11-query/#modOffset) clauses
* [REDUCED](http://www.w3.org/TR/sparql11-query/#modDuplicates) modifier
* SPARQL 1.1's [NOT](http://www.w3.org/TR/sparql11-query/#negation), [Property Paths](http://www.w3.org/TR/sparql11-query/#propertypaths), [assignment](http://www.w3.org/TR/sparql11-query/#assignment) (BIND / AS / VALUES), [subqueries](http://www.w3.org/TR/sparql11-query/#subqueries)
* SPARQL 1.1 [Federated Query](http://www.w3.org/TR/sparql11-federated-query/) syntax
