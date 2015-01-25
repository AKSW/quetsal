----Startup Information---------

package org.aksw.simba.quetzal.startup
Class ExecuteQuery

Note: The SPARQL 1.0 to SPARQL 1.1 query rewrite is currently based on string matching which is highly sensitive to query syntax. We will change it by implementing Sesame visitor. 
Currently, the query shouldn't use any prefixes. Rather the complete URIs should be used in triple patterns and also there should be exactly a single space between tuples  (s, p, o)
of triple patterns. FedBench queries are already there in the startup page. 

If you find any bug, please report at saleem.muhammd@gmail.com. Suggestions are welcome. 