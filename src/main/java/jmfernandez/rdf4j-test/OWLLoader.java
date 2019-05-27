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
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

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
			repo.initialize();
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
	
	private final static String[] ontologyURIs = {
		"http://purl.obolibrary.org/obo/obi/2018-08-27/obi.owl",
		"http://purl.obolibrary.org/obo/cl/releases/2018-07-07/cl.owl",
		"https://www.ebi.ac.uk/efo/releases/v2018-02-15/efo.owl",
		"http://purl.obolibrary.org/obo/uberon/releases/2018-07-30/uberon.owl"
	};
	
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
			
			for(String ontoURIStr: ontologyURIs) {
				Repository repo = initializeRepo(manager,ontoURIStr);
				try(RepositoryConnection con = repo.getConnection()) {
					System.err.println(ontoURIStr + " Size: "+con.size());
				} catch(RDF4JException e) {
					// handle exception. This catch-clause is
					// optional since rdf4jException is an unchecked exception
					throw e;
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}