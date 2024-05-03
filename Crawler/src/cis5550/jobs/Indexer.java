package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;


public class Indexer {
	
	private ConcurrentHashMap<String, ArrayList<FlamePair>> map = new ConcurrentHashMap<String, ArrayList<FlamePair>>();
	
	private static String stem(String input) {
		
		PorterStemmer stemmer = new PorterStemmer();
		stemmer.add(input.toCharArray(), input.length());
	    stemmer.stem();
	    return stemmer.toString();

    }
	
	private static void extractContent(Matcher matcher, List<String> content) {
        while (matcher.find()) {
            String text = matcher.group(1);
            text = text.replaceAll("<[^>]+>", " ");  // Remove all HTML tags
            text = text.replaceAll("[^\\x00-\\x7F]", " ");  // Remove non-ASCII characters
            text = text.replaceAll("[^a-zA-Z ]", " ").toLowerCase();  // Remove all non-alphabetic characters, convert to lower case
            text = text.replaceAll("\\s+", " ");  // Replace multiple whitespace with single space

            String[] tokens = text.trim().split("\\s+");
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    content.add(token);
                }
            }
        }
    }
	
	public static void run(FlameContext context, String[] args) throws Exception
	{
		try {
			FlameRDD tableUrl = context.fromTable("pt-crawl", row -> {
				String u = row.get("url");
				String hashedU = Hasher.hash(u);
				String p = row.get("page");
				
				System.out.println("url " + u + " hashed u " + hashedU);
				//System.out.println("p " + p);
				
	            return u+","+p; 
	        });
			
			int totalUrlCount = tableUrl.count();
			
			FlamePairRDD flamePair = tableUrl.mapToPair(input -> {
				String[] stringPair = input.split(",", 2);
				FlamePair pair = new FlamePair(stringPair[0], stringPair[1]);
				return pair;
			});
			FlamePairRDD wordPair = flamePair.flatMapToPair(pair -> {
				String url = pair._1();
				String page = pair._2();
				
//				page = page.replaceAll("(?s)<script[^>]*>(.*?)</script>", " ");
//				page = page.replaceAll("(?s)<style[^>]*>(.*?)</style>", " ");
				
//				String html = page.replaceAll("(?i)<nav[^>]*>(.*?)</nav>", " ");
//			    html = html.replaceAll("(?i)<footer[^>]*>(.*?)</footer>", " ");
//			    html = html.replaceAll("(?i)<head[^>]*>(.*?)</head>", " ");
////			    html = html.replaceAll("(?i)<form[^>]*>(.*?)</form>", " ");
//			    html = html.replaceAll("(?i)<input[^>]*>", " ");
//			    html = html.replaceAll("(?i)<textarea[^>]*>(.*?)</textarea>", " ");
//			    html = html.replaceAll("(?i)<button[^>]*>(.*?)</button>", " ");
////			    html = html.replaceAll("(?i)<select[^>]*>(.*?)</select>", " ");
//			    html = html.replaceAll("<!--(.*?)-->", " ");
			    
				 
			    
			    String html = page.replaceAll("(?is)<script[^>]*>(.*?)</script>", " ");
		        html = html.replaceAll("(?is)<style[^>]*>(.*?)</style>", " ");
		        html = html.replaceAll("(?is)<head[^>]*>(.*?)</head>", " ");
		        html = html.replaceAll("(?is)<footer[^>]*>(.*?)</footer>", " ");
		        html = html.replaceAll("(?is)<nav[^>]*>(.*?)</nav>", " ");
		        html = html.replaceAll("(?is)<form[^>]*>(.*?)</form>", " ");
		        html = html.replaceAll("(?is)<(input|textarea|button|select)[^>]*>", " ");
		        html = html.replaceAll("(?is)<!--.*?-->", " ");
				
//				String text = page.replaceAll("<[^>]+>", " ");
//				String formatted = text.replaceAll("\\t\\r\\n", " ");
//				String onlyEnglish = formatted.replaceAll("[^\\x00-\\x7F]", " ");
//				String alphabets = onlyEnglish.replaceAll("[^a-zA-Z ]", " ");
//	            String words = alphabets.replaceAll("\\p{Punct}", " ").toLowerCase().replaceAll("[0-9]", " ");
	
	            
	            
//	            Pattern pattern = Pattern.compile("<(h[1-6]|p)[^>]*>(.*?)</\\1>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
//	            Matcher matcher = pattern.matcher(html);
//
//	            while (matcher.find()) {
//	                String text = matcher.group(2);
//	                text = text.replaceAll("<[^>]+>", " "); 
//	                text.replaceAll("[^\\x00-\\x7F]", " ");
//	                text = text.replaceAll("[^a-zA-Z ]", " ").toLowerCase(); 
//	                text.replaceAll("\\t\\r\\n", " ");
//	                
//	                String[] tokens = text.split("\\s+");
//	                for (String token : tokens) {
//	                    if (!token.isEmpty()) {
//	                        content.add(token);
//	                    }
//	                }
//	            }
		        
		        List<String> contentHeadings = new ArrayList<>();
		        
		        Pattern headingPattern = Pattern.compile("<h[1-6][^>]*>(.*?)</h[1-6]>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		        Matcher headingMatcher = headingPattern.matcher(html);
		        extractContent(headingMatcher, contentHeadings);
		        
		        String[] wordList = new String[contentHeadings.size()];
	            wordList = contentHeadings.toArray(wordList);
	            

	            
	            Map<String, Double> frequencyMap = new ConcurrentHashMap<>();
	            for (String word : wordList) {
	                if (!word.isEmpty()) {
	                    //word = stem(word);
	                    frequencyMap.put(word, frequencyMap.getOrDefault(word, 0.0) + 1);
	                }
	            }
	            
	            List<String> contentBody = new ArrayList<>();
		        
		        Pattern paragraphPattern = Pattern.compile("<p[^>]*>(.*?)</p>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		        Matcher paragraphMatcher = paragraphPattern.matcher(html);
		        extractContent(paragraphMatcher, contentBody);
	            
//	            String[] wordList = words.split("\\s+");
		        String[] wordListPara = new String[contentBody.size()];
		        wordListPara = contentBody.toArray(wordListPara);
	            
	            for (String word : wordListPara) {
	                if (!word.isEmpty()) {
	                    //word = stem(word);
	                    frequencyMap.put(word, frequencyMap.getOrDefault(word, 0.0) + 0.6);
	                }
	            }
	            
	            String[] result = new String[wordList.length + wordListPara.length];

	            System.arraycopy(wordList, 0, result, 0, wordList.length);

	            System.arraycopy(wordListPara, 0, result, wordList.length, wordListPara.length);
	            
	            List<FlamePair> iterList = new ArrayList<>();
	            System.out.println("url " + url);
	            System.out.println("total words " + (result.length - 1));
	            
	            for (String word : frequencyMap.keySet()) {
	            	System.out.println("word: " + word);
	            	System.out.println("current value " + frequencyMap.get(word));
	            	System.out.println("current in double " + frequencyMap.get(word) * 1.0);
	            	double value = (frequencyMap.get(word)*1.0)/(result.length - 1);
	            	System.out.println("final val " + value);
	            	String strVal = url + "," + String.valueOf(value);
	                FlamePair newPair = new FlamePair(word, strVal); 
	                iterList.add(newPair);
	            }
	            
	            
	            
	            Iterable<FlamePair> iter = iterList;
	            
				return iter;
			});
			
			
			FlamePairRDD numDocs = wordPair.foldByKey("0", (accumulator, currVal) -> {
			int acc = Integer.parseInt(accumulator);
	        //int curr = Integer.parseInt(currVal);
	        
	        int newAccRank = 0;
	        if(currVal != null) {
	        	newAccRank= acc + 1;
	        }
	        
	
	        return String.valueOf(newAccRank);
	    });
			
			
			//numDocs.saveAsTable("pt-numdocs");
			
			
			FlamePairRDD joinedTable = wordPair.join(numDocs);
			
			//joinedTable.saveAsTable("pt-joined");
			
			FlamePairRDD computeVal = joinedTable.flatMapToPair(pair ->{
				
				String key = pair._1();
				String val = pair._2();
				
				System.out.println("key " + key + " val " + val);
				
				List<FlamePair> iterList = new ArrayList<>();
				
				
				String [] parts = val.split(",", 3);
				
				System.out.println("parts[2] " + parts[2]);
				
				double den = Double.parseDouble(parts[2]);
				System.out.println("total url count " + totalUrlCount + " den " + den);
				double logVal = Math.log(totalUrlCount / den);
				
				System.out.println("log val " + logVal);
				
				System.out.println("parts[1] " + parts[1]);
				
				double tf = Double.parseDouble(parts[1]);
				
				System.out.println("tf " + tf);
				
				String result = String.valueOf(tf * logVal);
				
				FlamePair newPair = new FlamePair(key, result);
	            
				iterList.add(newPair);
	            
				KVSClient k = context.getKVS();
	
				k.put("pt-computed", key, parts[0], result);
				
//				Row row = new Row(key);
//				Set<String> rows = row.columns();
//				System.out.println("*** row size *** " + rows.size());
				
	            return Collections.emptyList();
				
			});
			

			
			//System.out.println("agg " + aggregate);
			//wordPair.saveAsTable("pt-index");
			//System.out.println("table saved");
	} catch(Exception e) {
		System.out.println("Exception occured " + e);
	}

	}
	
	
	

	

}
