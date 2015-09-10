package org.aksw.simba.quetzal.startup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.aksw.simba.quetzal.configuration.QuetzalConfig;
import org.aksw.simba.quetzal.core.QueryRewriting;
import org.aksw.simba.quetzal.core.TBSSSourceSelection;
import org.aksw.sparql.query.algebra.helpers.BGPGroupGenerator;
import org.aksw.sparql.query.algebra.helpers.QueryModelVisitorSourceSelector;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.repository.RepositoryException;
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
public class ExecuteTBSSQuery {

    public static void main(String[] args) throws Exception {
        //org.openrdf.query.algebra.Service
        //mainClaus(args);

        testService();
    }

    public static void testService() throws Exception {
        String queryStr = "SELECT * { SERVICE <http://foo> { ?s ?p ?o } }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(queryStr, null);

        System.out.println(query);

        SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        String roundtrip = renderer.render(query);
        System.out.println("Roundtrip: " + roundtrip);
    }

    public static void mainClaus(String[] args) throws Exception {
        long strtTime = System.currentTimeMillis();
        String FedSummaries = "summaries/FedBench-TBSS-b4.n3";
       //  String FedSummaries = "C://slices/Linked-SQ-DBpedia-Aidan.ttl";
        String mode = "ASK_dominant";  //{ASK_dominant, Index_dominant}
        double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources
        QuetzalConfig.initialize(FedSummaries,mode,commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode.
        System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));
        FedX fed = FederationManager.getInstance().getFederation();
        List<Endpoint> members = fed.getMembers();
        Cache cache =FederationManager.getInstance().getCache();
        List<String> queries = Queries.getFedBenchQueries();
        SPARQLRepository repo = new SPARQLRepository(members.get(0).getEndpoint());
        repo.initialize();
        int tpsrces = 0;
        int count = 0;
        for (String query : queries)
        {
            System.out.println("-------------------------------------\n"+query);
            long startTime = System.currentTimeMillis();
            TBSSSourceSelection sourceSelection = new TBSSSourceSelection(members,cache, query);


            SPARQLParser parser = new SPARQLParser();
            ParsedQuery queryObj = parser.parseQuery(query, null);
            TupleExpr expr = queryObj.getTupleExpr();
            ParsedQuery parsedQuery = parser.parseQuery(query, null);

            QueryModelVisitorSourceSelector rewriter = new QueryModelVisitorSourceSelector(sourceSelection);
            //rewriter.meet(expr);
            expr.visit(rewriter);

            ParsedTupleQuery pq = new ParsedTupleQuery(expr);
            System.out.println("ParsedQuery: " + pq);


            String rewriteStr = new SPARQLQueryRenderer().render(pq);

            System.out.println("Rewritten query: " + rewriteStr);
            System.exit(0);

//            HashMap<Integer, List<StatementPattern>> bgpGroups =  BGPGroupGenerator.generateBgpGroups(query);
//            Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.performSourceSelection(bgpGroups);
//            //  System.out.println(DNFgrps)
//            System.out.println("Source selection exe time (ms): "+ (System.currentTimeMillis()-startTime));
//
//                        for (StatementPattern stmt : stmtToSources.keySet())
//                        {
//                            tpsrces = tpsrces+ stmtToSources.get(stmt).size();
//                            //System.out.println("-----------\n"+stmt);
//                            //System.out.println(stmtToSources.get(stmt));
//                        }
//
//            count =  executeQuery(query,bgpGroups,stmtToSources,repo);
            System.out.println(": Query execution time (msec):"+ (System.currentTimeMillis()-startTime));
            System.out.println("Total results: " + count);
            Thread.sleep(1000);

        }
        System.out.println("NetTriple pattern-wise selected sources after step 2 of HIBISCuS source selection : "+ tpsrces);
        FederationManager.getInstance().shutDown();
        System.exit(0);
    }

    public static void mainOriginal(String[] args) throws Exception {
        long strtTime = System.currentTimeMillis();
        String FedSummaries = "summaries/FedBench-TBSS-b4.n3";
       //  String FedSummaries = "C://slices/Linked-SQ-DBpedia-Aidan.ttl";
        String mode = "ASK_dominant";  //{ASK_dominant, Index_dominant}
        double commonPredThreshold = 0.33 ;  //considered a predicate as common predicate if it is presenet in 33% available data sources
        QuetzalConfig.initialize(FedSummaries,mode,commonPredThreshold);  // must call this function only one time at the start to load configuration information. Please specify the FedSum mode.
        System.out.println("One time configuration loading time : "+ (System.currentTimeMillis()-strtTime));
        FedX fed = FederationManager.getInstance().getFederation();
        List<Endpoint> members = fed.getMembers();
        Cache cache =FederationManager.getInstance().getCache();
        List<String> queries = Queries.getFedBenchQueries();
        SPARQLRepository repo = new SPARQLRepository(members.get(0).getEndpoint());
        repo.initialize();
        int tpsrces = 0;
        int count = 0;
        for (String query : queries)
        {
            System.out.println("-------------------------------------\n"+query);
            long startTime = System.currentTimeMillis();
            TBSSSourceSelection sourceSelection = new TBSSSourceSelection(members,cache, query);
            HashMap<Integer, List<StatementPattern>> bgpGroups =  BGPGroupGenerator.generateBgpGroups(query);
            Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.performSourceSelection(bgpGroups);
            //  System.out.println(DNFgrps)
            System.out.println("Source selection exe time (ms): "+ (System.currentTimeMillis()-startTime));

                        for (StatementPattern stmt : stmtToSources.keySet())
                        {
                            tpsrces = tpsrces+ stmtToSources.get(stmt).size();
                            //System.out.println("-----------\n"+stmt);
                            //System.out.println(stmtToSources.get(stmt));
                        }

            count =  executeQuery(query,bgpGroups,stmtToSources,repo);
            System.out.println(": Query execution time (msec):"+ (System.currentTimeMillis()-startTime));
            System.out.println("Total results: " + count);
            Thread.sleep(1000);

        }
        System.out.println("NetTriple pattern-wise selected sources after step 2 of HIBISCuS source selection : "+ tpsrces);
        FederationManager.getInstance().shutDown();
        System.exit(0);
    }
    /**
     * Execute query and return the number of results
     * @param query SPARQL 	query
     * @param bgpGroups BGPs
     * @param stmtToSources Triple Pattern to sources
     * @param repo  repository
     * @return Number of results
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     */
    public static int executeQuery(String query, HashMap<Integer, List<StatementPattern>> bgpGroups, Map<StatementPattern, List<StatementSource>> stmtToSources, SPARQLRepository repo) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        String newQuery = QueryRewriting.doQueryRewriting(query,bgpGroups,stmtToSources);
        //	System.out.println(newQuery);
            TupleQuery tupleQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, newQuery);
            int count = 0;
            TupleQueryResult result = tupleQuery.evaluate();
            while(result.hasNext())
            {
                //System.out.println(result.next());
                result.next();
                count++;
            }

        return count;
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
