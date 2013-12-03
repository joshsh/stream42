<!-- This README can be viewed at https://github.com/joshsh/sesamestream/wiki -->

![SesameStream logo|width=94px|height=65px](https://github.com/joshsh/sesamestream/wiki/graphics/sesamestream-logo-small.png)

SesameStream is a continuous SPARQL query engine for real-time applications, built with the [Sesame](http://www.openrdf.org/) RDF framework.  It implements a minimal subset of the [SPARQL](http://www.w3.org/TR/sparql11-query/) query language (namely, SELECT queries for basic graph patterns) and matches streaming RDF data against these queries with very low latency, responding to individual statements with the query answers they complete.  The query engine conserves resources by indexing only those statements which match against already-submitted queries, making it appropriate for processing long, noisy data streams.  SesameStream was developed for use in [wearable and ubiquitous computing](https://github.com/joshsh/extendo) contexts in which sensor data must be combined with background semantics under real time constraints of a few tens of milliseconds.

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
