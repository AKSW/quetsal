----Startup Information---------
You can run Quetsal in 4 modes: Hibiscus, TBSS, Hibiscus+DAW, and TBSS+DAW. DAW is to provide the duplicate-aware functionality to a given triple pattern-wise source selection. 

package org.aksw.simba.quetzal.startup
1. Class ExecuteHibiscusQuery  (Execute Quetsal-Hibiscus queries)
2. Class ExecuteTBSSQuery      (Execute Quetsal-TBSS queries)

You just need to enable the DAW flag in both of the execution in order to provide duplicate-aware aware functionality. For now we have disabled the query execution, thus if you directly run the the above files it will print the source selection results. If you want to run complete query then just remove the comment from //    count =  executeQuery(query,bgpGroups,stmtToSources,repo); . Note In order to run queries, you need to start all the required FedBench SPARQL endpoints and replace the endpoints URLs in the corresponding summaries. 

Note: The SPARQL 1.0 to SPARQL 1.1 query rewrite is currently based on string matching which is highly sensitive to query syntax. We will change it by implementing Sesame visitor. 
Currently, the query shouldn't use any prefixes. Rather the complete URIs should be used in triple patterns and also there should be exactly a single space between tuples  (s, p, o)
of triple patterns. FedBench queries are already there in the startup page. 

If you report any bug, please report at saleem.muhammd@gmail.com. Suggestions are welcome. 