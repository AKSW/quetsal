package org.aksw.sparql.query.algebra.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.aksw.simba.quetzal.core.TBSSSourceSelection;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.queryrender.sparql.SparqlTupleExprRenderer;

import com.fluidops.fedx.algebra.StatementSource;

public class QueryModelVisitorSourceSelector
    extends QueryModelVisitorBGPBase
{
    protected TBSSSourceSelection sourceSelection;
    protected int bgpId = 0;

    public QueryModelVisitorSourceSelector(
            TBSSSourceSelection sourceSelection) {
        super();
        this.sourceSelection = sourceSelection;
    }

    public static TupleExpr unify(Collection<TupleExpr> exprs) {
        TupleExpr result;
        if(exprs.isEmpty()) {
            result = null;
        } else if(exprs.size() == 1) {
            result = exprs.iterator().next();
        } else {
            result = null;
            for(TupleExpr item : exprs) {
                result = result == null ? item : new Union(result, item);
            }
        }
        return result;
    }

    public static TupleExpr doQueryRewriting(List<StatementPattern> bgp, Map<StatementPattern, List<StatementSource>> stmtToSources) throws Exception {

        TupleExpr result = null;

        Map<String, List<StatementPattern>> excGroups = new ConcurrentHashMap<String, List<StatementPattern>>();

        int exCount = 0 ;
        //excGroups.clear();



        for(StatementPattern stmt : bgp)
        {
            stmt = stmt.clone();
            //String triplePattern = getTriplePattern(stmt);
            List<StatementSource> sources = stmtToSources.get(stmt);
            //System.out.println(triplePattern + " , " +sources);
//            if(sources.size() == 1)
//            {
//                String sourceURL ="http://"+ sources.get(0).getEndpointID().replace("sparql_", "").replace("_", "/");
//                if(excGroups.containsKey(sourceURL))
//                {
//                    List<StatementPattern> excStmts = excGroups.get(sourceURL);
//                    synchronized (excStmts)
//                    {
//                        excStmts.add(stmt);
//                    }
//                }
//                else
//                {
//                    List<StatementPattern> excStmts = new ArrayList<StatementPattern>();
//                    excStmts.add(stmt);
//                    excGroups.put(sourceURL, excStmts);
//                }
//            }
//            else
            {
                //String services = "";
                List<TupleExpr> members = new ArrayList<TupleExpr>();
                for(StatementSource src : sources)
                {
                    String sourceURL ="http://"+ src.getEndpointID().replace("sparql_", "").replace("_", "/");
                    ValueFactory valueFactory = new ValueFactoryImpl();
                    URI serviceValue = valueFactory.createURI(sourceURL);
                    Var serviceVar = new Var("serviceVar", serviceValue);
                    //stmt.
                    //String patternString = "SERVICE <FOO> { " + new SparqlTupleExprRenderer().render(stmt) + "}";
                    String patternString = new SparqlTupleExprRenderer().render(stmt);

                    patternString = "SERVICE <" + sourceURL + "> {" + patternString + "}";

                    //System.out.println("PatternString: " + patternString);
                    Service service = new Service(serviceVar, stmt, patternString, Collections.<String, String>emptyMap(), null, false);

                    members.add(service);
                }

                result = unify(members);


                //System.out.println(services+"\n");
                //replace();
                //query= query.replace(triplePattern, services);
            }
        }

        //System.out.println(excGroups);
//        for(String serviceURL : excGroups.keySet())
//        {
//            String service ="";
//            List<StatementPattern> triples = excGroups.get(serviceURL);
//            if(triples.size()==1)
//            {
//                service = "{ SERVICE  <"+serviceURL + "> { "+triples.get(0) + " } }";
//                query =  query.replace(triples.get(0), service);
//            }
//            else
//            {  exCount = exCount+triples.size()-1;
//            service = "{\n   SERVICE  <"+serviceURL + "> {\n   ";
//            int count = 0;
//            for(String triple:triples)
//            {
//                service = service+ triple+"\n   ";
//                count++;
//                if(count<triples.size())
//                    query =  query.replace(triple+"\n", "");
//                else
//                {
//                    service = service+" }\n  }";
//                    query = query.replace(triple, service);
//                }
//            }
//
//            }
//
//        }

        return result;
    }




    @Override
    public void meetBGP(TupleExpr node, List<StatementPattern> bgp) {


//        System.out.println("Meeting BGP " + node);
//        System.out.println("my parent is " + node.getParentNode());

        HashMap<Integer, List<StatementPattern>> map = new HashMap<>();
        map.put(bgpId++, bgp);

        try {
            Map<StatementPattern, List<StatementSource>> stmtToSources = sourceSelection.performSourceSelection(map);
            TupleExpr replacement = doQueryRewriting(bgp, stmtToSources);
            node.replaceWith(replacement);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }



//        System.out.println("GOT BGP of size " + bgp.size());
//        System.out.println(bgp);
//        System.out.println("=======");
//
//        StatementPattern p = bgp.iterator().next();
//        node.replaceWith(p);
    }

}
