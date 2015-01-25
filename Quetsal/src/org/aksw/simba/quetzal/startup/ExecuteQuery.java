package org.aksw.simba.quetzal.startup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aksw.simba.quetzal.configuration.QuetzalConfig;
import org.aksw.simba.quetzal.core.QueryRewriting;
import org.aksw.simba.quetzal.core.QuetzalSourceSelection;
import org.aksw.sparql.query.algebra.helpers.BGPGroupGenerator;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.sparql.SPARQLRepository;

import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.structures.Endpoint;
/**
 * Execute Query
 * @author Saleem
 *
 */
public class ExecuteQuery {
	public static void main(String[] args) throws Exception {
		long strtTime = System.currentTimeMillis();
		String FedSummaries = "summaries\\quetzal-b4.n3";

		String mode = "ASK_dominant";  //{ASK_dominant, Index_dominant}
		double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources

		QuetzalConfig.initialize(FedSummaries,mode,commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode. 
		System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));

		FedX fed = FederationManager.getInstance().getFederation();
		List<Endpoint> members = fed.getMembers();
		Cache cache =FederationManager.getInstance().getCache();
		//	File Querydir = new File("D:/BigRDFBench/queries/");
		//	File[] listOfQueryFiles = Querydir.listFiles();
		List<String> queries = new ArrayList<String> ();
		String S1 = "SELECT ?predicate ?object  \n" //cd1 of fedbench
				+ "WHERE \n"
				+ " { \n" +    
				"     { \n"
				+ "     <http://dbpedia.org/resource/Barack_Obama> ?predicate ?object . \n"
				+ "     }\n" +
				" UNION \n " +
				" { \n"
				+ "   ?subject <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n" +
				"   ?subject ?predicate ?object .\n"
				+ "  } \n " +
				"}";
		String S2 = "SELECT ?party ?page  WHERE { \n" +   //cd2
				" <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party .\n" +
				" ?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
				"?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n"+
				"}";
		String S3 = "SELECT ?president ?party ?page WHERE { \n" + //cd3
				"?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> .\n" +
				"?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> .\n" +
				"?president <http://dbpedia.org/ontology/party> ?party .\n" +
				"?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
				"?x <http://www.w3.org/2002/07/owl#sameAs> ?president .\n" +
				"}";

		String S4 = "SELECT ?actor ?news WHERE {\n"+   //cd4
				"?film <http://purl.org/dc/terms/title> \"Tarzan\" .\n"+
				"?film <http://data.linkedmdb.org/resource/movie/actor> ?actor .\n"+
				"?actor <http://www.w3.org/2002/07/owl#sameAs> ?x .\n"+
				"?y <http://www.w3.org/2002/07/owl#sameAs> ?x .\n"+
				"?y <http://data.nytimes.com/elements/topicPage> ?news . \n"+
				"}";
		String S5 = "SELECT ?film ?director ?genre WHERE {\n"+    //cd5 
				"?film <http://dbpedia.org/ontology/director> ?director .\n"+
				"?director <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/Italy> .\n"+
				"?x <http://www.w3.org/2002/07/owl#sameAs> ?film .\n"+
				"?x <http://data.linkedmdb.org/resource/movie/genre> ?genre .\n"+
				"}";
		String S6 = "SELECT ?name ?location WHERE {\n"+ //cd 6
				"?artist <http://xmlns.com/foaf/0.1/name> ?name .\n"+
				"?artist <http://xmlns.com/foaf/0.1/based_near> ?location .\n"+
				"?location <http://www.geonames.org/ontology#parentFeature> ?germany . \n"+
				"?germany <http://www.geonames.org/ontology#name> \"Federal Republic of Germany\" . \n"+
				"}";
		String S7= "SELECT ?location ?news WHERE {\n"+ //cd7
				"?location <http://www.geonames.org/ontology#parentFeature> ?parent .\n"+ 
				"?parent <http://www.geonames.org/ontology#name> \"California\" .\n"+
				"?y <http://www.w3.org/2002/07/owl#sameAs> ?location .\n"+
				"?y <http://data.nytimes.com/elements/topicPage> ?news . \n "+
				"}";
		//-----------------------------------------LS queries of FedBench-----------------------------------
		String S8 = "SELECT ?drug ?melt WHERE {\n"+  //LS1
				"{ ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint> ?melt . }\n"+
				"    UNION"+
				"    { ?drug <http://dbpedia.org/ontology/Drug/meltingPoint> ?melt . \n}"+
				"}";
		String S9 = "SELECT ?predicate ?object WHERE {\n"+ //LS2
				"{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> ?predicate ?object . }\n"+
				"UNION    "+
				"{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> <http://www.w3.org/2002/07/owl#sameAs> ?caff .\n"+
				"  ?caff ?predicate ?object . } \n"+
				"}";
		String S10 = "SELECT ?Drug ?IntDrug ?IntEffect WHERE { \n"+ //LS3
				" ?Drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Drug> .\n"+
				" ?y <http://www.w3.org/2002/07/owl#sameAs> ?Drug .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1> ?y .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2> ?IntDrug .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text> ?IntEffect . \n"+
				"}";

		String S11 = "SELECT ?drugDesc ?cpd ?equation WHERE {\n"+  //LS4
				"?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/cathartics> .\n"+
				"   ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?cpd .\n"+
				"  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description> ?drugDesc .\n"+
				" ?enzyme <http://bio2rdf.org/ns/kegg#xSubstrate> ?cpd .\n"+
				"?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> .\n"+
				"?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme .\n"+
				"?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . \n"+
				"}";
		String S12 = "SELECT ?drug ?keggUrl ?chebiImage WHERE {\n"+ //LS5
				"?drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs> .\n"+
				"  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?keggDrug .\n"+
				"  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#url> ?keggUrl .\n"+
				" ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName> ?drugBankName .\n"+
				" ?chebiDrug <http://purl.org/dc/elements/1.1/title> ?drugBankName .\n"+
				" ?chebiDrug <http://bio2rdf.org/ns/bio2rdf#image> ?chebiImage .\n"+
				"}" ;
		String S13 = "SELECT ?drug ?title WHERE {\n "+ //LS6
				"?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> .\n"+
				" ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?id .\n"+
				" ?keggDrug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Drug> .\n"+
				" ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id .\n"+
				" ?keggDrug <http://purl.org/dc/elements/1.1/title> ?title .\n"+
				"}";
		String S14 = "SELECT ?drug ?transform ?mass WHERE { \n "+ //LS7
				"{ ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism> \"Humans and other mammals\" .\n"+
				" 	  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?cas .\n"+
				"	  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?cas .\n"+
				"	  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> ?mass . \n"+
				"	} "+
				"}";

		queries.add(S1);
		queries.add(S2);
		queries.add(S3);
		queries.add(S4);
		queries.add(S5);
		queries.add(S6);
		queries.add(S7);
		queries.add(S8);
		queries.add(S9);
		queries.add(S10);
		queries.add(S11);
		queries.add(S12);
		queries.add(S13);
		queries.add(S14);
		SPARQLRepository repo = new SPARQLRepository(members.get(0).getEndpoint());
		repo.initialize();
		int tpsrces = 0; 
		for (String query : queries)
		{
			System.out.println("-------------------------------------\n"+query);
			//String query = getQuery(qryFile);
			//String query = S14;
			long startTime = System.currentTimeMillis();
			QuetzalSourceSelection sourceSelection = new QuetzalSourceSelection(members,cache, query);
			HashMap<Integer, List<StatementPattern>> bgpGroups =  BGPGroupGenerator.generateBgpGroups(query);
			Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.performSourceSelection(bgpGroups);
			//  System.out.println(DNFgrps)
			System.out.println("Source selection exe time (ms): "+ (System.currentTimeMillis()-startTime));

			//			for (StatementPattern stmt : stmtToSources.keySet()) 
			//			{
			//				tpsrces = tpsrces+ stmtToSources.get(stmt).size();
			//				System.out.println("-----------\n"+stmt);
			//				System.out.println(stmtToSources.get(stmt));
			//			}

			String newQuery = QueryRewriting.doQueryRewriting(query,bgpGroups,stmtToSources);
			System.out.println(newQuery);
			TupleQuery tupleQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, newQuery); 
			int count = 0;
			TupleQueryResult result = tupleQuery.evaluate();
			while(result.hasNext())
			{
				//System.out.println(result.next());
				result.next();
				count++;
			}
			System.out.println(": Query execution time (msec):"+ (System.currentTimeMillis()-startTime));
			System.out.println("Total results: " + count);
			System.out.println("Rewriting: " + (System.currentTimeMillis()-startTime));
			Thread.sleep(1000);

		}	
		System.out.println("NetTriple pattern-wise selected sources after step 2 of HIBISCuS source selection : "+ tpsrces);
		FederationManager.getInstance().shutDown();
		System.exit(0);
	}

	@SuppressWarnings("unused")
	private static void printParseQuery(String query) throws MalformedQueryException {
		SPARQLParserFactory factory = new SPARQLParserFactory();
		QueryParser parser = factory.getParser();
		ParsedQuery parsedQuery = parser.parseQuery(query, null);
		System.out.println(parsedQuery.toString());


	}

	/**
	 * Load query string from file
	 * @param qryFile Query File
	 * @return query Query string
	 * @throws IOException IO exceptions
	 */
	public static String  getQuery(File qryFile) throws IOException {
		String query = "" ; 
		BufferedReader br = new BufferedReader(new FileReader(qryFile));
		String line;
		while ((line = br.readLine()) != null)
		{
			query = query+line+"\n";
		}
		br.close();
		return query;
	}
}
