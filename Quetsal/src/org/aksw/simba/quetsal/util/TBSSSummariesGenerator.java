package org.aksw.simba.quetsal.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.aksw.simba.quetzal.core.TBSSSourceSelection;
import org.aksw.simba.quetzal.datastructues.Trie;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 *  Generate Quetzal-TBSS Summaries for a set of federation members (SPARQL endpoints)
 *  @author saleem
 */
public class TBSSSummariesGenerator {
	public static  BufferedWriter bw ;
	public static double distinctSbj;
	/**
	 * initialize input information for data summaries generation
	 * @param location Directory location of the resulting FedSummaries file (i.e. location/FedSum.n3)
	 * @throws IOException IO Exceptions
	 */
	public TBSSSummariesGenerator(String location) throws IOException 
	{
		bw= new BufferedWriter(new FileWriter(new File(location))); //--name/location where the summaries file will be stored
		bw.append("@prefix ds:<http://aksw.org/quetsal/> .");
		bw.newLine();
	}
	public static void main(String[] args) throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException {
		List<String> endpoints = 	(Arrays.asList(
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ0",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ1",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ2",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ3",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ4",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ5",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ6",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ7",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ8",
				//			   "http://localhost:8080/openrdf-sesame/repositories/UOBM-univ9"
//			         	      "http://localhost:8890/sparql",
//							  "http://localhost:8891/sparql",
//							 "http://localhost:8892/sparql",
//							 "http://localhost:8893/sparql",
//							 "http://localhost:8894/sparql",
//							 "http://localhost:8895/sparql",
//							 "http://localhost:8896/sparql",
//							 "http://localhost:8897/sparql",
							 "http://localhost:8898/sparql"
							// "http://localhost:8899/sparql"
				));


		String outputFile = "summaries/quetsal-Fedbench-8898-b4.n3";
		String namedGraph = "http://aksw.org/fedbench/";  //can be null. in that case all graph will be considered 
		TBSSSummariesGenerator generator = new TBSSSummariesGenerator(outputFile);
		long startTime = System.currentTimeMillis();
		int branchLimit =4;
		generator.generateSummaries(endpoints,namedGraph,branchLimit);
		System.out.println("Data Summaries Generation Time (min): "+ (System.currentTimeMillis()-startTime)/(1000*60));
		System.out.print("Data Summaries are secessfully stored at "+ outputFile);

		//	outputFile = "summaries\\quetzal-b2.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =2;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
		//	
		//	outputFile = "summaries\\quetzal-b5.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =5;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
		//	
		//	outputFile = "summaries\\quetzal-b10.n3";
		//	generator = new SummariesGenerator(outputFile);
		//	startTime = System.currentTimeMillis();
		//	branchLimit =10;
		//	generator.generateSummaries(endpoints,namedGraph,branchLimit);
		//	System.out.println("Data Summaries Generation Time (sec): "+ (System.currentTimeMillis()-startTime)/1000);
		//	System.out.print("Data Summaries are secessfully stored at "+ outputFile);
	}
	/**
	 * Build Quetzal data summaries for the given list of SPARQL endpoints
	 * @param endpoints List of SPARQL endpoints url
	 * @param graph Named graph. Can be null. In this case all named graphs will be considered for Quetzal summaries
	 * @param branchLimit Branching limit
	 * @throws IOException Io Error
	 * @throws QueryEvaluationException Query Execution Error 
	 * @throws MalformedQueryException  Memory Error
	 * @throws RepositoryException  Repository Error
	 */
	public void generateSummaries(List<String> endpoints, String graph, int branchLimit) throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException{
		for(String endpoint:endpoints)
		{
			System.out.println("generating summaries for: " + endpoint);
			long totalTrpl=0, totalSbj=0, totalObj=0;
			ArrayList<String> lstPred = getPredicates(endpoint,graph);
			System.out.println("total distinct predicates: "+ lstPred.size());
			bw.append("#---------------------"+endpoint+" Summaries-------------------------------");
			bw.newLine();
			bw.append("[] a ds:Service ;");
			bw.newLine();
			bw.append("     ds:url   <"+endpoint+"> ;");
			bw.newLine();
			for(int i =0 ;i<lstPred.size();i++)
			{
				System.out.println((i+1)+" in progress: " + lstPred.get(i));
				bw.append("     ds:capability");
				bw.newLine();
				bw.append("         [");
				bw.newLine();
				bw.append("           ds:predicate  <"+lstPred.get(i)+"> ;");
				writeSbjPrefixes(lstPred.get(i),endpoint,graph,branchLimit);
				long trpleCount = getTripleCount(lstPred.get(i),endpoint);
				if(distinctSbj==0)
					distinctSbj=trpleCount;
				bw.append("\n           ds:avgSbjSelectivity  "+ (1/distinctSbj)+" ;");
				totalSbj = (long) (totalSbj+distinctSbj);
				writeObjPrefixes(lstPred.get(i),endpoint,graph,branchLimit);
				double distinctObj = getObj(lstPred.get(i),endpoint);
				bw.append("\n           ds:avgObjSelectivity  "+ (1/distinctObj)+" ;");
				totalObj = (long) (totalObj+distinctObj);
				bw.newLine();
				bw.append("           ds:triples    "+trpleCount+" ;");
				bw.append("         ] ;");
				bw.newLine();
				totalTrpl = totalTrpl+trpleCount;
			}
			bw.append("     ds:totalSbj "+totalSbj+" ; \n");  // this is not representing the actual number of distinct subjects in a datasets since the same subject URI can be shaared by more than one predicate. but we keep this to save time.  
			bw.append("     ds:totalObj "+totalObj+" ; \n");  
			bw.append("     ds:totalTriples "+totalTrpl+" ; \n");
			bw.newLine();
			bw.append("             .");
			bw.newLine();

		}
		//     bw.append("     sd:totalTriples \""+totalTrpl+"\" ;");
		bw.append("#---------End---------");
		bw.close();
	}
	/**
	 * Get total number of triple for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static double getObj(String pred, String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		double triples = 0;
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objs) " + // 
				"WHERE " +
				"{" +

	       		"?s <"+pred+"> ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			triples = Long.parseLong(rs.next().getValue("objs").stringValue());

		}
		return triples;
	}
	/**
	 * Get total number of distinct subjects of a dataset
	 * @return count
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static Long getSubjectsCount(String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		long count = 0;
		String strQuery = "SELECT  (COUNT(DISTINCT ?s) AS ?sbjts) " + // 
				"WHERE " +
				"{" +

	       		"?s ?p ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			count = Long.parseLong(rs.next().getValue("sbjts").stringValue());

		}
		return count;
	}
	/**
	 * Get total number of distinct objects of a dataset
	 * @return count
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static Long getObjectsCount(String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		long count = 0;
		String strQuery = "SELECT  (COUNT(DISTINCT ?o) AS ?objts) " + // 
				"WHERE " +
				"{" +

	       		"?s ?p ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			count = Long.parseLong(rs.next().getValue("objts").stringValue());

		}
		return count;
	}
	/**
	 * Get total number of triple for a predicate
	 * @param pred Predicate
	 * @param m model
	 * @return triples
	 * @throws RepositoryException 
	 * @throws MalformedQueryException 
	 * @throws QueryEvaluationException 
	 */
	public static Long getTripleCount(String pred, String endpoint) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		long triples = 0;
		String strQuery = "SELECT  (COUNT(?s) AS ?triples) " + // 
				"WHERE " +
				"{" +

	       		"?s <"+pred+"> ?o " +
	       		"} " ;
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult rs = query.evaluate();
		while( rs.hasNext() ) 
		{
			triples = Long.parseLong(rs.next().getValue("triples").stringValue());

		}
		return triples;
	}
	/**
	 * Write all the distinct subject prefixes  for triples with predicate p. 
	 * @param predicate  Predicate
	 * @param endpoint Endpoint URL
	 * @param graph named Graph
	 * @param branchLimit Branching Limit for Trie
	 * @throws MalformedQueryException  Query Error
	 * @throws RepositoryException  Repository Error
	 * @throws QueryEvaluationException  Query Execution Error
	 * @throws IOException  IO Error
	 */
	public void writeSbjPrefixes(String predicate, String endpoint, String graph, int branchLimit) throws RepositoryException, MalformedQueryException, QueryEvaluationException, IOException {
		ArrayList<String> sbjAuthorities = new ArrayList<String>();
		Map<String, List<String>> authGroups = new ConcurrentHashMap<String, List<String>>();
		String strQuery = "";
		if(graph== null)
			strQuery = getSbjAuthorityQury(predicate);
		else
			strQuery = getSbjAuthorityQury(predicate,graph);
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		try{
			TupleQueryResult res = query.evaluate();
			int rsCount = 0; 
			while (res.hasNext()) 
			{
				String curSbj = res.next().getValue("s").toString();
				rsCount++;
				//System.out.println(curSbj);
				String[] sbjPrts = curSbj.split("/");
				if ((sbjPrts.length>1))
				{
					try{
						String sbjAuth =sbjPrts[0]+"//"+sbjPrts[2];
						//if(!sbjAuthorities.contains(sbjAuth))
						//	sbjAuthorities.add(sbjAuth);
						//---------
						if(authGroups.containsKey(sbjAuth))
						{
							List<String> URIs = authGroups.get(sbjAuth);
							synchronized (URIs)
							{
								URIs.add(curSbj);
							}
						}
						else
						{
							List<String> newURIs = new ArrayList<String>();	
							newURIs.add(curSbj);
							authGroups.put(sbjAuth, newURIs);
						}
					}
					catch (Exception e){
						System.err.println("Subject is not a valid URI. Subject prefix ignored");
					}
				}
			}
			distinctSbj = rsCount;
		}
		catch (Exception e){
			System.err.println("Subject is not a valid URI. Subject Authority ignored");
		}
		repo.getConnection().close();
		//sbjAuthorities = Prefix.getLCPs(authGroups);

		sbjAuthorities = Trie.getAllBranchingURIs(authGroups, branchLimit);
		if(!sbjAuthorities.isEmpty())
		{
			bw.newLine();
			bw.append("           ds:sbjPrefix ");

			for(int authority=0; authority<sbjAuthorities.size();authority++)
			{
				String strAuth = sbjAuthorities.get(authority);
				if(authority==sbjAuthorities.size()-1)
					bw.write("<"+strAuth.replace(" ", "") + "> ; ");
				else
					bw.write("<"+ strAuth.replace(" ", "")+ ">, ");
			}
		}
	}
	///**
	// * Get a SPARQL query to retrieve all the subject authorities for a predicate
	// * Note: Due to a limit of 10000 results per query on a SPARQL endpoint, we are using Regular expressions in queries
	// * to get the required part in each qualifying triples rather than doing a local SPLIT operation on results
	// * @param predicate predicate
	// * @return query Required SPARQL query
	// */
	//public String getSbjAuthorityQury(String predicate) {
	//	
	//	String query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
	//				+ "WHERE \n"
	//				+ "{ \n "
	//				+ "   ?s <"+predicate+"> ?o. \n"
	//				+ "  BIND(STRBEFORE(str(?s),REPLACE(str(?s), \"^([^/]*/){3}\", \"\")) AS ?authPath) \n"
	//						+ "   Filter(isURI(?s)) \n"
	//						+ "}" ;
	//		return query;
	//}
	/**
	 *  Get a SPARQL query to retrieve all distinct subjects for retrieving all distinct subject authorities for a predicate
	 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greather than that limit
	 * @param predicate Predicate
	 * @return query Required SPARQL query
	 */
	public String getSbjAuthorityQury(String predicate) {
		String query = "SELECT DISTINCT ?s  \n"
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?s)) \n"   //only URI can have authority
				+ "}" ;

		return query;
	}
	/**
	 *  Get a SPARQL query to retrieve all distinct subjects for retrieving all distinct subject authorities for a predicate
	 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greater than that limit
	 * @param predicate Predicate
	 * @param graph Named graph
	 * @return query Required SPARQL query
	 */
	public String getSbjAuthorityQury(String predicate, String graph) {
		String query = "SELECT DISTINCT ?s  FROM <" +graph + "> "
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?s)) \n"   //only URI can have authority
				+ "}" ;

		return query;
	}
	/**
	 * Write all the distinct object authorities having predicate p. 
	 * @param predicate  Predicate
	 * @param endpoint Endpoint URL
	 * @param graph named Graph
	 * @param branchLimit Branching limit for Trie
	 * @throws MalformedQueryException Query Error
	 * @throws RepositoryException  Repository Error
	 * @throws QueryEvaluationException  Query Execution Error
	 * @throws IOException  IO Error
	 */
	public void writeObjPrefixes(String predicate, String endpoint, String graph, int branchLimit) throws RepositoryException, MalformedQueryException, QueryEvaluationException, IOException {
		ArrayList<String> objAuthorities = new ArrayList<String>();
		Map<String, List<String>> authGroups = new ConcurrentHashMap<String, List<String>>();
		String strQuery = "";
		if(graph==null)
			strQuery = getObjAuthorityQury(predicate);
		else
			strQuery = getObjAuthorityQury(predicate,graph);
		SPARQLRepository repo = new SPARQLRepository(endpoint);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		try{
			TupleQueryResult res = query.evaluate();
			while (res.hasNext()) 
			{
				if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
					objAuthorities.add(res.next().getValue("o").toString());
				else
				{
					String Obj =  res.next().getValue("o").toString();
					// System.out.println("obj:" + Obj);
					String[] objPrts = Obj.split("/");
					if ((objPrts.length>1))
					{
						try{

							String objAuth =objPrts[0]+"//"+objPrts[2];
							//if(!objAuthorities.contains(objAuth))
							//	objAuthorities.add(objAuth);  
							if(authGroups.containsKey(objAuth))
							{
								List<String> URIs = authGroups.get(objAuth);
								synchronized (URIs)
								{
									URIs.add(Obj);
								}
							}
							else
							{
								List<String> newURIs = new ArrayList<String>();	
								newURIs.add(Obj);
								authGroups.put(objAuth, newURIs);
							}
						}
						catch (Exception e){
							System.err.println("Problem with object URI. Object authorithy ignored for that uri");
						}
					}
				}
			}
		}
		catch (Exception e){
			System.err.println("Problem with object URI. Object prefix ignored for that uri");
		}
		repo.getConnection().close();
		//objAuthorities.addAll(Prefix.getLCPs(authGroups));
		objAuthorities = TBSSSourceSelection.getUnion(objAuthorities,Trie.getAllBranchingURIs(authGroups, branchLimit));
		if(!objAuthorities.isEmpty())
		{
			bw.newLine();
			bw.append("           ds:objPrefix ");

			for(int authority=0; authority<objAuthorities.size();authority++)
			{
				String strAuth = objAuthorities.get(authority);
				if(authority==objAuthorities.size()-1)
					bw.write("<"+strAuth.replace(" ", "") + "> ; ");
				else
					bw.write("<"+ strAuth.replace(" ", "")+ ">, ");
			}
		}
	}
	///**
	// * Get a SPARQL query to retrieve all the object authorities for a predicate
	// * Note: Due to a limit of 10000 results per query on a SPARQL endpoint, we are using Regular expressions in queries
	// * to get the required part in each qualifying triples rather than doing a local SPLIT operation on results. For
	// * rdf:type we retrieve the complete set of objects as usually the number of distinct classes in a dataset is usually not too many.
	// * @param predicate Predicate
	// * @return query Required SPARQL query
	// */
	//public String getObjAuthorityQury(String predicate) {
	//	String query;
	//	if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
	//	{
	//		 query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
	//				+ "WHERE \n"
	//				+ "{  \n"
	//				+ "   ?s <"+predicate+"> ?authPath. \n"
	//			
	//						+ "}" ;
	//	}
	//	
	//	else
	//	{
	//	 query = "SELECT DISTINCT ?authPath From <http://deri.benchmark> \n"
	//			+ "WHERE \n"
	//			+ "{  \n"
	//			+ "   ?s <"+predicate+"> ?o. \n"
	//			+ "  BIND(STRBEFORE(str(?o),REPLACE(str(?o), \"^([^/]*/){3}\", \"\")) AS ?authPath) \n "
	//					+ "   Filter(isURI(?o)) \n"
	//					+ "}" ;
	//	}
	//		return query;
	//}
	/**
	 *  Get a SPARQL query to retrieve all distinct objects for retrieving all distinct object authorities for a predicate
	 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greather than that limit
	 * @param predicate Predicate
	 * @return query Required SPARQL query
	 */
	public String getObjAuthorityQury(String predicate) {
		String query = "SELECT DISTINCT ?o    \n"
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?o)) \n"   //only URI can have authority
				+ "}" ;

		return query;
	}
	/**
	 *  Get a SPARQL query to retrieve all distinct objects for retrieving all distinct object authorities for a predicate
	 * Note: You need to increase the 1000 limit of results for SPARQL endpoints if the distinct subjects for a predicate is greather than that limit
	 * @param predicate Predicate
	 * @param graph Named graph
	 * @return query Required SPARQL query
	 */
	public String getObjAuthorityQury(String predicate, String graph) {
		String query = "SELECT DISTINCT ?o   FROM <" +graph + "> "
				+ "WHERE \n"
				+ "{  \n"
				+ "   ?s <"+predicate+"> ?o. "
				+ "   Filter(isURI(?o)) \n"   //only URI can have authority
				+ "}" ;

		return query;
	}
	/**
	 * Get Predicate List
	 * @param endPointUrl SPARQL endPoint Url
	 * @param graph Named graph
	 * @return  predLst Predicates List
	 * @throws MalformedQueryException 
	 * @throws RepositoryException 
	 * @throws QueryEvaluationException 
	 */
	private static ArrayList<String> getPredicates(String endPointUrl, String graph) throws RepositoryException, MalformedQueryException, QueryEvaluationException 
	{
		ArrayList<String>  predLst = new ArrayList<String>();
		String strQuery = "";
		if(graph==null)
			strQuery = getPredQury();
		else
			strQuery = getPredQury(graph);
		SPARQLRepository repo = new SPARQLRepository(endPointUrl);
		repo.initialize();
		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, strQuery); 
		TupleQueryResult res = query.evaluate();
		while (res.hasNext()) 
		{   String pred = res.next().getValue("p").toString();
		predLst.add(pred);	  		
		}
		repo.getConnection().close();
		return predLst;
	}

	//--------------------------------------------------------------------------
	/**
	 * Get SPARQL query to retrieve all predicates in a SAPRQL endpoint
	 * @return query SPARQL query
	 */
	private static String getPredQury() {
		String query = "SELECT DISTINCT ?p  "
				+ " WHERE "
				+ "{"
				+ "	?s ?p ?o"
				+ "} " ;
		return query;
	}
	/**
	 * Get SPARQL query to retrieve all predicates in a SAPRQL endpoint
	 * @param graph Named Graph
	 * @return query SPARQL query
	 */
	private static String getPredQury(String graph) {
		String query = "SELECT DISTINCT ?p FROM <" +graph + "> "
				+ " WHERE "
				+ "{"
				+ "	?s ?p ?o"
				+ "} " ;
		return query;
	}
}