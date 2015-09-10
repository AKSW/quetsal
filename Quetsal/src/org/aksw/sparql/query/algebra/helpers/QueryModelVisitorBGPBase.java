package org.aksw.sparql.query.algebra.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * A QueryModelVisitor that collects StatementPattern's from a query model.
 * StatementPatterns thet are part of filters/constraints are not included in
 * the result.
 */
public abstract class QueryModelVisitorBGPBase extends QueryModelVisitorBase<RuntimeException> {
    public abstract void meetBGP(TupleExpr node, List<StatementPattern> bgp);

    @Override
    public void meet(Join node)
    {
        List<StatementPattern> bgp = collectBGP(node);
        if(bgp == null) {
            meetNode(node);
        } else {
            meetBGP(node, bgp);
        }
    }

    @Override
    public void meet(StatementPattern node)
    {
        List<StatementPattern> bgp = collectBGP(node);
        meetBGP(node, bgp);
    }

    public static List<StatementPattern> collectBGP(TupleExpr node) {
        List<StatementPattern> result = new ArrayList<>();

        try {
            collectBGP(node, result);
        } catch(Exception e) {
            result = null;
        }

        return result;
    }

    public static void collectBGP(TupleExpr node, Collection<StatementPattern> result) {
        if(node instanceof Join) {
            Join join = (Join) node;
            collectBGP(join.getLeftArg(), result);
            collectBGP(join.getRightArg(), result);
        } else if(node instanceof StatementPattern) {
            result.add((StatementPattern)node);
        } else {
            throw new RuntimeException("not a basic graph pattern");
        }
    }


}

