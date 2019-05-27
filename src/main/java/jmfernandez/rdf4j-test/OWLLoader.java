package jmfernandez.rdf4j_test;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.math.BigInteger;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.Value;

import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;

import org.eclipse.rdf4j.RDF4JException;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;

import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryInfo;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import org.eclipse.rdf4j.repository.sail.SailRepository;

import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;

import org.eclipse.rdf4j.rio.RDFFormat;

import org.eclipse.rdf4j.sail.config.SailImplConfig;

import org.eclipse.rdf4j.sail.nativerdf.config.NativeStoreConfig;

import org.eclipse.rdf4j.sail.nativerdf.NativeStore;


public final class OWLLoader {
	// Borrowed from https://stackoverflow.com/a/332101
	public static String toHexString(byte[] bytes) {
		StringBuilder hexString = new StringBuilder();
		
		for (int i = 0; i < bytes.length; i++) {
			String hex = Integer.toHexString(0xFF & bytes[i]);
			if (hex.length() == 1) {
				hexString.append('0');
			}
			hexString.append(hex);
		}
		
		return hexString.toString();
	}
	
	// Borrowed from https://stackoverflow.com/a/943963
	public static String toHex(byte[] bytes) {
		BigInteger bi = new BigInteger(1, bytes);
		return String.format("%0" + (bytes.length << 1) + "X", bi);
	}
	
	public static Repository initializeRepo(RepositoryManager manager, String ontoStr)
		throws IOException, NoSuchAlgorithmException
	{
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		byte[] repByteId = md.digest(ontoStr.getBytes("UTF-8"));
		String repId = DatatypeConverter.printHexBinary(repByteId);
		
		RepositoryInfo repInfo = manager.getRepositoryInfo(repId);
		if(repInfo == null) {
			// create a configuration for the SAIL stack
			SailImplConfig backendConfig = new NativeStoreConfig();
			// create a configuration for the repository implementation
			RepositoryImplConfig repositoryTypeSpec = new SailRepositoryConfig(backendConfig);
			
			RepositoryConfig repConfig = new RepositoryConfig(repId, repositoryTypeSpec);
			manager.addRepositoryConfig(repConfig);
			
			repInfo = manager.getRepositoryInfo(repId);
		}
		
		Repository repo = manager.getRepository(repId);
		
		if(!repo.isInitialized()) {
			repo.init();
		}
		
		URL ontoURL = new URL(ontoStr);
		try(RepositoryConnection con = repo.getConnection()) {
			if(con.isEmpty()) {
				HttpURLConnection ontoConn = getURLConnection(ontoURL);
				try(InputStream ontoIS = ontoConn.getInputStream()) {
					con.add(ontoIS, ontoStr, RDFFormat.RDFXML);
				} finally {
					ontoConn.disconnect();
				}
			}
		} catch(RDF4JException e) {
			// handle exception. This catch-clause is
			// optional since rdf4jException is an unchecked exception
			throw e;
		} catch(IOException e) {
			// handle io exception
			throw e;
		}
		
		return repo;
	}
	
	// This manages redirects, inspired in https://stackoverflow.com/a/26046079
	public final static HttpURLConnection getURLConnection(URL resourceUrl)
		throws IOException
	{
		HttpURLConnection conn;
		Map<String, Integer> visited = new HashMap<>();

		while(true) {
			String url = null;
			String proto = resourceUrl.getProtocol();
			switch(proto) {
				case "http":
				case "https":
				case "ftp":
					url = resourceUrl.toExternalForm();
					int times = visited.compute(url, (key, count) -> count == null ? 1 : count + 1);
					
					if (times > 3) {
						throw new IOException("Stuck in redirect loop in URL "+url);
					}
					break;
				default:
					throw new IOException("Unknown protocol '"+proto+"'");
			}
			
			conn = (HttpURLConnection) resourceUrl.openConnection();
			
			//conn.setConnectTimeout(15000);
			//conn.setReadTimeout(15000);
			conn.setInstanceFollowRedirects(false);   // Make the logic below easier to detect redirections
			//conn.setRequestProperty("User-Agent", "Mozilla/5.0...");
			
			switch(conn.getResponseCode()) {
				case HttpURLConnection.HTTP_MOVED_PERM:
				case HttpURLConnection.HTTP_MOVED_TEMP:
					String location = conn.getHeaderField("Location");
					location = URLDecoder.decode(location, "UTF-8");
					
					conn.disconnect();
					
					URL base = resourceUrl;               
					URL next = new URL(base, location);  // Deal with relative URLs
					resourceUrl = next;
					continue;
			}
			break;
		}
		
		return conn;
	}
	
	
	public final static void main(String[] args) {
		try {
			File baseDir = new File("_NATIVE_RDF");
			baseDir.mkdirs();
			LocalRepositoryManager manager = new LocalRepositoryManager(baseDir);
			manager.initialize();
			
			SimpleValueFactory svf = SimpleValueFactory.getInstance();
			
			
			Repository repo;
			String ontoURIStr;
			
			System.out.println("First");
			ontoURIStr = "https://www.ebi.ac.uk/efo/releases/v2018-02-15/efo.owl";
			repo = initializeRepo(manager,ontoURIStr);
			try {
				try(RepositoryConnection con = repo.getConnection()) {
					System.err.println(ontoURIStr + " Size: "+con.size());
					
					TupleQuery tupleQuery;
					TupleQueryResult result;
					
					// Match by IRI
					System.out.println("\tSearch by IRI");
					String queryStringIRI = "SELECT ?q WHERE {\n"+
					"{ ?q rdf:type owl:Class }\n"+
					"UNION\n"+
					"{ ?q rdf:type rdfs:Class }\n"+
					"} ";
					
					tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryStringIRI);
					tupleQuery.setBinding("q",svf.createIRI("http://www.ebi.ac.uk/efo/EFO_0003042"));
					
					result = tupleQuery.evaluate();
					try(TupleQueryResult tupleRes = tupleQuery.evaluate()) {
						while(tupleRes.hasNext()) {  // iterate over the result
							BindingSet bindingSet = tupleRes.next();
							Value valueOfQ = bindingSet.getValue("q");
							System.out.println("\t\t"+valueOfQ.stringValue());
						}
					}
					
					// Match by suffix
					System.out.println("\tSearch by IRI suffix");
					String queryStringSuf = "SELECT ?x WHERE {\n"+
					"{ ?x rdf:type owl:Class }\n"+
					"UNION\n"+
					"{ ?x rdf:type rdfs:Class }\n"+
					"FILTER strends(str(?x),?q) .\n"+
					"} ";
					
					tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryStringSuf);
					tupleQuery.setBinding("q",svf.createLiteral("EFO_0003042"));
					
					result = tupleQuery.evaluate();
					try(TupleQueryResult tupleRes = tupleQuery.evaluate()) {
						while(tupleRes.hasNext()) {  // iterate over the result
							BindingSet bindingSet = tupleRes.next();
							Value valueOfX = bindingSet.getValue("x");
							System.out.println("\t\t"+valueOfX.stringValue());
						}
					}
					
					// Negative match by suffix
					System.out.println("\tSearch by IRI suffix (no result)");
					
					tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryStringSuf);
					tupleQuery.setBinding("q",svf.createLiteral("EFO_000304"));
					
					result = tupleQuery.evaluate();
					try(TupleQueryResult tupleRes = tupleQuery.evaluate()) {
						while(tupleRes.hasNext()) {  // iterate over the result
							BindingSet bindingSet = tupleRes.next();
							Value valueOfX = bindingSet.getValue("x");
							System.out.println("\t\t"+valueOfX.stringValue());
						}
					}
					
				} catch(RDF4JException e) {
					// handle exception. This catch-clause is
					// optional since rdf4jException is an unchecked exception
					throw e;
				}
			} finally {
				repo.shutDown();
			}
			
			System.out.println("Second");
			ontoURIStr = "http://purl.obolibrary.org/obo/cl/releases/2018-07-07/cl.owl";
			repo = initializeRepo(manager,ontoURIStr);
			try {
				try(RepositoryConnection con = repo.getConnection()) {
					System.err.println(ontoURIStr + " Size: "+con.size());
				} catch(RDF4JException e) {
					// handle exception. This catch-clause is
					// optional since rdf4jException is an unchecked exception
					throw e;
				}
			} finally {
				repo.shutDown();
			}
			
			System.out.println("Third");
			ontoURIStr = "http://purl.obolibrary.org/obo/uberon/releases/2018-07-30/uberon.owl";
			repo = initializeRepo(manager,ontoURIStr);
			try {
				try(RepositoryConnection con = repo.getConnection()) {
					System.err.println(ontoURIStr + " Size: "+con.size());
				} catch(RDF4JException e) {
					// handle exception. This catch-clause is
					// optional since rdf4jException is an unchecked exception
					throw e;
				}
			} finally {
				repo.shutDown();
			}
			
			System.out.println("Fourth");
			ontoURIStr = "http://purl.obolibrary.org/obo/obi/2018-08-27/obi.owl";
			repo = initializeRepo(manager,ontoURIStr);
			try {
				try(RepositoryConnection con = repo.getConnection()) {
					System.err.println(ontoURIStr + " Size: "+con.size());
					
					TupleQuery tupleQuery;
					TupleQueryResult result;
					
					// Match by label
					System.out.println("\tSearch by label");
					String queryStringLab = "SELECT ?x WHERE {\n"+
					"?x rdfs:label ?q .\n" +
					"{ ?x rdf:type owl:Class }\n"+
					"UNION\n"+
					"{ ?x rdf:type rdfs:Class } .\n"+
					"} ";
					
					tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryStringLab);
					tupleQuery.setBinding("q",svf.createLiteral("ChIP-seq assay"));
					
					result = tupleQuery.evaluate();
					try(TupleQueryResult tupleRes = tupleQuery.evaluate()) {
						while(tupleRes.hasNext()) {  // iterate over the result
							BindingSet bindingSet = tupleRes.next();
							Value valueOfX = bindingSet.getValue("x");
							System.out.println("\t\t"+valueOfX.stringValue());
						}
					}
					
					// Obtain ancestors
					System.out.println("\tObtain ancestors by label");
					String queryStringAnc = "SELECT ?x WHERE {\n"+
					"?i rdfs:label ?q .\n" +
					"{ ?i rdf:type owl:Class }\n"+
					"UNION\n"+
					"{ ?i rdf:type rdfs:Class } .\n"+
					"?i rdfs:subClassOf* ?x"+
					"} ";
					
					tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryStringAnc);
					tupleQuery.setBinding("q",svf.createLiteral("ChIP-seq assay"));
					
					result = tupleQuery.evaluate();
					try(TupleQueryResult tupleRes = tupleQuery.evaluate()) {
						while(tupleRes.hasNext()) {  // iterate over the result
							BindingSet bindingSet = tupleRes.next();
							Value valueOfX = bindingSet.getValue("x");
							System.out.println("\t\t"+valueOfX.stringValue());
						}
					}
				} catch(RDF4JException e) {
					// handle exception. This catch-clause is
					// optional since rdf4jException is an unchecked exception
					throw e;
				}
			} finally {
				repo.shutDown();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}