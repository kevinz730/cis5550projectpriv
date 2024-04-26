package cis5550.jobs;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
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
		int numDocs = k.count("pt-crawl");
		
		
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
				    System.out.println("queryIDF " + queryIDF);
			    	queryTF_IDF = queryTF.get(queryParts[i]) * queryIDF;
			    	System.out.println("queryTF_IDF " + queryTF_IDF);
			    	
			    	String tf_idf = r.get(col);
			    	System.out.println("tf_idf " + tf_idf);
			    	double docTF_IDF = Double.parseDouble(tf_idf);
			    	System.out.println("docTF_IDF " + docTF_IDF);
			    	
				    if(!computedVals.contains(col)) {
				    	System.out.println("!computedVals.contains(col) new val added " + tf_idf);
				    	computedVals.put(col, String.valueOf(docTF_IDF * queryTF_IDF));
				    }
				    else  {
				    	String prevVal = computedVals.get(col);
				    	System.out.println("prevVal " + prevVal);
				    	double prevValDouble = Double.parseDouble(prevVal);
				    	System.out.println("prevValDouble " + prevValDouble);
				    	double newVal = docTF_IDF* queryTF_IDF + prevValDouble;
				    	System.out.println("newVal " + String.valueOf(newVal));
				    	computedVals.put(col, String.valueOf(newVal));
				    }
				    docWeight += Math.pow(docTF_IDF, 2);
				    System.out.println("docWeight " + String.valueOf(docWeight));
				}
				
				double queryWeight = 0.0;
				for(Map.Entry<String, Double> e: queryTF.entrySet()) {
					queryWeight += Math.pow(queryTF.get(e.getKey())*queryIDF, 2);
					System.out.println("queryWeight in for loop " + String.valueOf(queryWeight));
				}
				queryWeight = Math.sqrt(queryWeight);
				System.out.println("queryWeight " + String.valueOf(queryWeight));
				
		    	//Doc weight
		    	docWeight = Math.sqrt(docWeight);
		    	System.out.println("docWeight after sqrt " + String.valueOf(docWeight));

		    	for(Map.Entry<String, String> e:computedVals.entrySet()) {
		    		String col = e.getKey();
		    		System.out.println("col " + col);
		    		double cosSim = Double.parseDouble(computedVals.get(col)) / (docWeight * queryWeight);
		    		System.out.println("cosSim " + String.valueOf(cosSim));
		    		
		    		String hashedVal = Hasher.hash(col);
				    Row pageRank = k.getRow("pt-pageranks", hashedVal);
				    String data = pageRank.get("rank");
				    System.out.println("data " + data);
				    double dataDouble = Double.parseDouble(data);
				    System.out.println("dataDouble " + String.valueOf(dataDouble));
			    	double newVal = dataDouble + cosSim;
			    	System.out.println("newVal " + String.valueOf(newVal));
			    	computedVals.put(col, String.valueOf(newVal));
		    	}
		    	
		    	//PageRank Calculation
		    	
			}
		}
		
		 computedVals.entrySet()
         .stream()
         .sorted(Map.Entry.<String, String>comparingByValue(Comparator.comparingDouble(Double::parseDouble).reversed()))
         .forEach(entry -> System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue()));

	}

}
