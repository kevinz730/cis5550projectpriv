package cis5550.jobs;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class ProcessQuery {
	
	public static void run(FlameContext context, String[] args) throws Exception{
		
		ConcurrentHashMap<String, Double> queryTF = new ConcurrentHashMap<>();
		String query = args[0];
		System.out.println("query: " + query);
		
		String[] queryParts = query.split(" ");
		for(String s: queryParts) {
			double freq = queryTF.getOrDefault(s, 0.0);
			queryTF.put(s, freq+1);
		}
		for(Map.Entry<String, Double> e: queryTF.entrySet()) {
			queryTF.put(e.getKey(), e.getValue()*1.0/query.length());
		}
		
		KVSClient k = context.getKVS();
		int numDocs = k.count("pt-crawl")+1;
		
		
		ConcurrentHashMap<String, String> computedVals = new ConcurrentHashMap();
		
		double queryIDF = 0.0;
		double queryTF_IDF = 0.0;
		double docWeight = 0.0;
		
		for(int i=0; i<queryParts.length; i++) {
			
			//TF-IDF Calculation
			if(k.existsRow("pt-computed", queryParts[i])) 
			{
				Row r = k.getRow("pt-computed", queryParts[i]);
				Set<String> cols = r.columns();
				int numDocsWithTerm = cols.size();
				int j = 0;
				for (String col : cols) {
				    System.out.println("j " + j + " col " + col);
				    j++; 
				    queryIDF = (numDocs)*1.0/numDocsWithTerm;
			    	queryTF_IDF = queryTF.get(queryParts[i]) * queryIDF;
			    	String tf_idf = r.get(col);
			    	double docTF_IDF = Double.parseDouble(tf_idf);
			    	
				    if(!computedVals.contains(col)) {
//				    	String tf_idf = r.get(col);
				    	
				    	//String stringVal = new String(tf_idf, StandardCharsets.UTF_8);
				    	System.out.println("new val added " + tf_idf);
//				    	double queryIDF = (numDocs+1)*1.0/numDocsWithTerm;
//				    	double queryTF_IDF = queryTF.get(queryParts[i]) * queryIDF;
				    	computedVals.put(col, String.valueOf(docTF_IDF * queryTF_IDF));
				    }
				    else 
				    {
//				    	String tf_idf = r.get(col);
				    	//String stringVal = new String(tf_idf, StandardCharsets.UTF_8);
//				    	double doubleVal = Double.parseDouble(tf_idf);
				    	String prevVal = computedVals.get(col);
				    	double prevValDouble = Double.parseDouble(prevVal);
				    	double newVal = docTF_IDF* queryTF_IDF + prevValDouble;
				    	System.out.println("prev val updated " + String.valueOf(newVal));
				    	computedVals.put(col, String.valueOf(newVal));
				    }
				    docWeight += Math.pow(docTF_IDF, 2);
				    
				    double queryWeight = 0.0;
					for(Map.Entry<String, Double> e: queryTF.entrySet()) {
						queryWeight += Math.pow(queryTF.get(e.getKey())*queryIDF, 2);
					}
					queryWeight = Math.sqrt(queryWeight);
					
			    	//Doc weight
			    	docWeight = Math.sqrt(docWeight);

			    	double cosSim = Double.parseDouble(computedVals.get(col)) / (docWeight * queryWeight);
			    	
			    	//PageRank Calculation
			    	String hashedVal = Hasher.hash(col);
				    Row pageRank = k.getRow("pt-pageranks", hashedVal);
				    String data = pageRank.get("rank");
				    System.out.println("data " + data);
				    double dataDouble = Double.parseDouble(data);
				    String prevVal = computedVals.get(col);
			    	double prevValDouble = Double.parseDouble(prevVal);
			    	double newVal = dataDouble + prevValDouble;
			    	System.out.println("value update after pagerank " + String.valueOf(newVal));
			    	computedVals.put(col, String.valueOf(newVal));
				}
			}
		}
		
		for(Map.Entry<String, Double> e: queryTF.entrySet())
		{
			
		}
		
		 computedVals.entrySet()
         .stream()
         .sorted(Map.Entry.<String, String>comparingByValue(Comparator.comparingDouble(Double::parseDouble).reversed()))
         .forEach(entry -> System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue()));

	}

}
