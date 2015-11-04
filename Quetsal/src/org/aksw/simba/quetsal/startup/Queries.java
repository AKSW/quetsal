package org.aksw.simba.quetsal.startup;

import java.util.ArrayList;
import java.util.List;

public class Queries {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
    
	}
/**
 * Get FedBench Queries
 * @return List of queries
 */
	public static List<String> getFedBenchQueries() {
		List<String> queries = new ArrayList<String> ();
		String CD1 = "SELECT ?predicate ?object  \n" //cd1 of fedbench
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
		String CD2 = "SELECT ?party ?page  WHERE { \n" +   //cd2
				" <http://dbpedia.org/resource/Barack_Obama> <http://dbpedia.org/ontology/party> ?party .\n" +
				" ?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
				"?x <http://www.w3.org/2002/07/owl#sameAs> <http://dbpedia.org/resource/Barack_Obama> .\n"+
				"}";
		String CD3 = "SELECT ?president ?party ?page WHERE { \n" + //cd3
				"?president <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/President> .\n" +
				"?president <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/United_States> .\n" +
				"?president <http://dbpedia.org/ontology/party> ?party .\n" +
				"?x <http://data.nytimes.com/elements/topicPage> ?page .\n" +
				"?x <http://www.w3.org/2002/07/owl#sameAs> ?president .\n" +
				"}";

		String CD4 = "SELECT ?actor ?news WHERE {\n"+   //cd4
				"?film <http://purl.org/dc/terms/title> \"Tarzan\" .\n"+
				"?film <http://data.linkedmdb.org/resource/movie/actor> ?actor .\n"+
				"?actor <http://www.w3.org/2002/07/owl#sameAs> ?x .\n"+
				"?y <http://www.w3.org/2002/07/owl#sameAs> ?x .\n"+
				"?y <http://data.nytimes.com/elements/topicPage> ?news . \n"+
				"}";
		String CD5 = "SELECT ?film ?director ?genre WHERE {\n"+    //cd5 
				"?film <http://dbpedia.org/ontology/director> ?director .\n"+
				"?director <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/Italy> .\n"+
				"?x <http://www.w3.org/2002/07/owl#sameAs> ?film .\n"+
				"?x <http://data.linkedmdb.org/resource/movie/genre> ?genre .\n"+
				"}";
		String CD6 = "SELECT ?name ?location WHERE {\n"+ //cd 6
				"?artist <http://xmlns.com/foaf/0.1/name> ?name .\n"+
				"?artist <http://xmlns.com/foaf/0.1/based_near> ?location .\n"+
				"?location <http://www.geonames.org/ontology#parentFeature> ?germany . \n"+
				"?germany <http://www.geonames.org/ontology#name> \"Federal Republic of Germany\" . \n"+
				"}";
		String CD7= "SELECT ?location ?news WHERE {\n"+ //cd7
				"?location <http://www.geonames.org/ontology#parentFeature> ?parent .\n"+ 
				"?parent <http://www.geonames.org/ontology#name> \"California\" .\n"+
				"?y <http://www.w3.org/2002/07/owl#sameAs> ?location .\n"+
				"?y <http://data.nytimes.com/elements/topicPage> ?news . \n "+
				"}";
		//-----------------------------------------LS queries of FedBench-----------------------------------
		String LS1 = "SELECT ?drug ?melt WHERE {\n"+  //LS1
				"{ ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint> ?melt . }\n"+
				"    UNION"+
				"    { ?drug <http://dbpedia.org/ontology/Drug/meltingPoint> ?melt . \n}"+
				"}";
		String LS2 = "SELECT ?predicate ?object WHERE {\n"+ //LS2
				"{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> ?predicate ?object . }\n"+
				"UNION    "+
				"{ <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00201> <http://www.w3.org/2002/07/owl#sameAs> ?caff .\n"+
				"  ?caff ?predicate ?object . } \n"+
				"}";
		String LS3 = "SELECT ?Drug ?IntDrug ?IntEffect WHERE { \n"+ //LS3
				" ?Drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Drug> .\n"+
				" ?y <http://www.w3.org/2002/07/owl#sameAs> ?Drug .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug1> ?y .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/interactionDrug2> ?IntDrug .\n"+
				" ?Int <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/text> ?IntEffect . \n"+
				"}";

		String LS4 = "SELECT ?drugDesc ?cpd ?equation WHERE {\n"+  //LS4
				"?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/cathartics> .\n"+
				"   ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?cpd .\n"+
				"  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description> ?drugDesc .\n"+
				" ?enzyme <http://bio2rdf.org/ns/kegg#xSubstrate> ?cpd .\n"+
				"?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> .\n"+
				"?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme .\n"+
				"?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . \n"+
				"}";
		String LS5 = "SELECT ?drug ?keggUrl ?chebiImage WHERE {\n"+ //LS5
				"?drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs> .\n"+
				"  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?keggDrug .\n"+
				"  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#url> ?keggUrl .\n"+
				" ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName> ?drugBankName .\n"+
				" ?chebiDrug <http://purl.org/dc/elements/1.1/title> ?drugBankName .\n"+
				" ?chebiDrug <http://bio2rdf.org/ns/bio2rdf#image> ?chebiImage .\n"+
				"}" ;
		String LS6 = "SELECT ?drug ?title WHERE {\n "+ //LS6
				"?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/micronutrient> .\n"+
				" ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?id .\n"+
				" ?keggDrug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Drug> .\n"+
				" ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?id .\n"+
				" ?keggDrug <http://purl.org/dc/elements/1.1/title> ?title .\n"+
				"}";
		String LS7 = "SELECT ?drug ?transform ?mass WHERE { \n "+ //LS7
				"{ ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/affectedOrganism> \"Humans and other mammals\" .\n"+
				" 	  ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/casRegistryNumber> ?cas .\n"+
				"	  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#xRef> ?cas .\n"+
				"	  ?keggDrug <http://bio2rdf.org/ns/bio2rdf#mass> ?mass . \n"+
				"	} "+
				"}";

		queries.add(CD1);
		queries.add(CD2);
		queries.add(CD3);
		queries.add(CD4);
		queries.add(CD5);
		queries.add(CD6);
		queries.add(CD7);
		queries.add(LS1);
		queries.add(LS2);
		queries.add(LS3);
		queries.add(LS4);
		queries.add(LS5);
		queries.add(LS6);
		queries.add(LS7);
		return queries;
	}

}
