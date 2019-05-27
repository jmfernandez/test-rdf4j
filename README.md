# Testing RDF4J

This is a test project, checking different RDF4J technologies and APIs, like SAIL Native triplestore (for persistence and better scalability), RIO (for RDF/XML or OWL import) and its SPARQL query capabilities over the native triplestore.

In order to test all of these, you need Maven to compile and assemble the project, once you have cloned it. Once cloned, you have to run next commands:

```bash
mvn clean package appassembler:assemble
cd target/appassembler/bin
./OWLLoader
```

