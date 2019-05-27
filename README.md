# Testing RDF4J

This is a test project, checking different RDF4J technologies and APIs, like SAIL Native triplestore (for persistence and better scalability) and RIO (for RDF/XML or OWL import).

In order to test it, you need Maven. To compile and test it, you have to clone it and run next commands:

```bash
mvn clean package appassembler:assemble
cd target/appassembler/bin
./OWLLoader
```

