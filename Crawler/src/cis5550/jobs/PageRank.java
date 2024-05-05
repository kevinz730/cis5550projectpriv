////package cis5550.jobs;
////
////import java.io.IOException;
////import java.net.MalformedURLException;
////import java.net.URL;
////import java.util.ArrayList;
////import java.util.Collections;
////import java.util.LinkedHashSet;
////import java.util.List;
////import java.util.regex.Matcher;
////import java.util.regex.Pattern;
////
////import cis5550.flame.FlameContext;
////import cis5550.flame.FlamePair;
////import cis5550.flame.FlamePairRDD;
////import cis5550.flame.FlameRDD;
////import cis5550.kvs.KVSClient;
////import cis5550.tools.Hasher;
////import cis5550.tools.URLParser;
////
////public class PageRank {
////	
////	public static List<String> urlNormalizer(List<String> urls, String base) throws MalformedURLException {
////		
////		ArrayList<String> normalizedUrls = new ArrayList<String>();
////		try {
////		
////		String baseUrl = base;
////		for (String url : urls) {
////			String[] urlParts = URLParser.parseURL(url);
////			String protocol = urlParts[0];
////			String hostName = urlParts[1];
////			String port = urlParts[2];
////			String rest = urlParts[3];
////			if (rest.indexOf('#') > -1) {
////				rest = rest.substring(0, rest.indexOf('#'));
////				if (protocol == null && hostName == null && port == null && rest.equals("")) {
////					normalizedUrls.add(baseUrl);
////					continue;
////				}
////			}
////			if (rest.lastIndexOf('.') > -1) {
////				String end = rest.substring(rest.lastIndexOf('.'));
////				if (end.equals(".jpg") || end.equals(".jpeg") || end.equals(".gif") || end.equals(".png") || end.equals(".txt")) {
////					continue;
////				}
////			}
////			if (protocol == null && hostName == null && port == null) {
//////				Either relative or absolute path without hostname
//////				Cuts off part in base URL after last /
////				String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
////				if (rest.charAt(0) == '/') {
//////					absolute path
////					String[] baseUrlParts = URLParser.parseURL(baseUrl);
////					modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
////					normalizedUrls.add(modifiedBaseUrl+rest);
////					continue;
////				} else {
////					while (rest.substring(0, 3).equals("../")) {
////						modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
////						rest = rest.substring(3);
////					}
////					normalizedUrls.add(modifiedBaseUrl+"/"+rest);
////					continue;
////				}
////			}
////			else if (port == null) {
////				String modifiedBaseUrl = "";
////				if (protocol.equals("http")) {
////					modifiedBaseUrl = protocol + "://" + hostName + ":" + "80";
////				} else if (protocol.equals("https")){
////					modifiedBaseUrl = protocol + "://" + hostName + ":" + "443";
////				} else {
////					continue;
////				}
////				normalizedUrls.add(modifiedBaseUrl+rest);
////				continue;
////			}
////			normalizedUrls.add(url);
////		}
////		}catch(Exception e){
////			normalizedUrls.add("");
////		}
////		return normalizedUrls;
////
////		
////		 
////	}
////	
////
////	
////	public static List<String> extractUrls(String pageData) {
////		//System.out.println("page data " + pageData);
////        List<String> urls = new ArrayList<>();
////        //TODO: confirm matcher
////        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?(?i)href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
////        Matcher matcher = pattern.matcher(pageData);
////
////        while (matcher.find()) {
////            String url = matcher.group(1).trim();
////            System.out.println("matched url " + url);
////            urls.add(url);
////        }
////
////        return urls;
////    }
////	
////	
////	public static void run(FlameContext context, String[] stringArr) {
////		
////		String crawlTable = "pt-crawl";
////
////		System.out.println("crawlTable " + crawlTable);
////		try {
////			
////			
////			    FlameRDD stateTable = context.fromTable("pt-crawl", row -> {
////				String u = row.get("url");
////				String p = row.get("page");
////				System.out.println("url " + u);
////				List<String> urls = new ArrayList<>();
////				if(p!=null) {
////					byte[] b = p.getBytes();
////					//urls = urlExtract(b);
////					urls = extractUrls(p);
////				}
////				Hasher hasher = new Hasher();
////				
////				List<String> normalizedUrls = new ArrayList<>();
////				try {
////					normalizedUrls = urlNormalizer(urls, u);
////				} catch (MalformedURLException e) {
////					e.printStackTrace();
////				}
////				
////				LinkedHashSet<String> lset= new LinkedHashSet<>(); 
////				LinkedHashSet<String> unhashed= new LinkedHashSet<>(); 
////				for(int i = 0; i<normalizedUrls.size(); i++)
////				{
////					
////					if(!normalizedUrls.get(i).equals("")) {
////						String hashed = hasher.hash(normalizedUrls.get(i));
////	            		lset.add(hashed);
////	            		//lset.add(normalizedUrls.get(i));
////	            	}
////				}
////				
////				String l = String.join(",", lset);
////				String test = String.join(",", unhashed);
////	            
////				String hashedu = hasher.hash(u);
////	            return hashedu+",1.0,1.0,"+l; 
////				
////	        });
////			    
////			    stateTable.saveAsTable("pt-stateTable");
////			    
////			FlamePairRDD flamePair = stateTable.mapToPair(input -> {
////				System.out.println("flame pair");
////				String[] stringPair = input.split(",", 2);
////				FlamePair pair = new FlamePair(stringPair[0], stringPair[1]);
////				return pair;
////			});
////			flamePair.saveAsTable("pt-flamePair");
////			
////			while(true) {
////				
////				FlamePairRDD transferTable = flamePair.flatMapToPair(pair -> {
////		            String u = pair._1(); 
////		            String val = pair._2();
////		            String[] parts = val.split(",", 3); 
////		            
////		            double rc = Double.parseDouble(parts[0]); 
////		            double rp = Double.parseDouble(parts[1]);
////		            System.out.println("split vals "+ rc + " " +rp);
////		            String links = parts[2]; 
////		            String[] linkArray = links.split(","); 
////		            int n = linkArray.length; 
////		
////		            List<FlamePair> outputPairs = new ArrayList<>();
////		
////		            for (String li : linkArray) {
////		                if (!li.isEmpty()) { 
////		                    double v = 0.85 * rc / n; 		                    
////		                    FlamePair newPair = new FlamePair(li, String.valueOf(v));
////		                    System.out.println("pair added " + newPair);
////		                    System.out.println("curr u "+li);
////		                    outputPairs.add(newPair);
////		                }
////		            }
////		            FlamePair newPair0 = new FlamePair(u, String.valueOf(0.0));
////		            System.out.println("pair0 added " + newPair0);
////		            System.out.println("curr u "+u);
////		        	outputPairs.add(newPair0);
////		
////		            Iterable<FlamePair> iter = outputPairs;
////		            System.out.println("returning pairs");
////		            return iter;
////		        });
////				
////				transferTable.saveAsTable("pt-transferTable");
////				
////				
////				FlamePairRDD aggregatedRanks = transferTable.foldByKey("0.0", (accumulator, currVal) -> {
////		            double accRank = Double.parseDouble(accumulator);
////		            double currentRankValue = Double.parseDouble(currVal);
////		            
////		            double newAccRank = accRank + currentRankValue;
////		            
////		            return String.valueOf(newAccRank);
////		        });
////				
////				aggregatedRanks.saveAsTable("pt-aggregatedRanks");
////				
////				System.out.println("aggregate done");
////				FlamePairRDD joined = aggregatedRanks.join(flamePair);
////				System.out.println("join done");
////				
////				joined.saveAsTable("pt-joined");
////
////				
////				FlamePairRDD updatedStateTable = joined.flatMapToPair(joinedData -> {
////		            String urlHash = joinedData._1(); 
////		            String values = joinedData._2();
////		            
////		            System.out.println("urlHash " + urlHash);
////		            System.out.println("values " + values);
////		            
////		            String[] stateVals = values.split(",", 4); 
////		            stateVals[2] = stateVals[1];
////		            stateVals[1] = String.valueOf(Double.parseDouble(stateVals[0]) + 0.15);
////		            
////		            String newStateVals = stateVals[1] +","+ stateVals[2] +","+ stateVals[3];
////		            
////		            System.out.println("new state vals " + newStateVals);
////		            
////		            List<FlamePair> outputPairs = new ArrayList<>();
////		            FlamePair newPair = new FlamePair(urlHash, newStateVals);
////		            outputPairs.add(newPair);
////		            
////		            Iterable<FlamePair> iter = outputPairs;
////		            return iter;
////		        });
////				
////				updatedStateTable.saveAsTable("pt-updatedStateTable");
////
////				flamePair = updatedStateTable;
////
////				
////				FlameRDD rankChanges = flamePair.flatMap(pair -> {
////	                String[] ranks = pair._2().split(",", 3);
////	                double oldRank = Double.parseDouble(ranks[1]);
////	                double newRank = Double.parseDouble(ranks[0]);
////	                double rankChange = Math.abs(newRank - oldRank); 
////	                
////	                System.out.println("url for rank " + pair._1());
////	                System.out.println("old rank " + oldRank + " new rank "+ newRank + " rank change " + rankChange);
////	                
////	                List<String> outputPairs = new ArrayList<>();
////		            outputPairs.add(String.valueOf(rankChange));
////		            
////		            Iterable<String> iter = outputPairs;
////		            return iter;
////	
////	            });
////				
////				rankChanges.saveAsTable("pt-rankChanges");
////				
////				String maxRankChange = rankChanges.fold("0.0", (acc, value) -> {
////	            	double maxVal = Math.max(Double.parseDouble(acc), Double.parseDouble(value));
////	            	System.out.println("acc val "+acc+" value val "+value+" maxval "+maxVal);
////	            	return String.valueOf(maxVal);
////	            });
////
////		
////					double maxDiff = Double.parseDouble(maxRankChange);
////
////					//double convThreshold = Double.parseDouble(stringArr[0]);
////					double convThreshold = 0.01;
////					if(maxDiff < convThreshold) break;
////			}
////			
////			
////			
////			flamePair.flatMapToPair(pair -> {
////				
////				KVSClient k = context.getKVS();
////	            String url = pair._1();  
////	            System.out.println("url " + url);
////	            String[] values = pair._2().split(",", 3);
////	            System.out.println("values " + pair._2());
////	            double currentRank = Double.parseDouble(values[0]); 
////	            //KVSClient kvs = context.getKVS();
////	           // System.out.println("curr rank " + currentRank);
////				System.out.println("vals to put: url "+url+" rank "+String.valueOf(currentRank));
////	            k.put("pt-pageranks", url, "rank", String.valueOf(currentRank));
////	            
////	            return Collections.emptyList();
////	        });
////
////	
////		} catch (IOException e) {
////			e.printStackTrace();
////		} catch (Exception e) {
////			e.printStackTrace();
////		}
////
////	}
////
////
////}
//
//
//
//
//
////
////package cis5550.jobs;
////
////import java.util.ArrayList;
////import java.util.List;
////import java.util.Set;
////import java.util.concurrent.ConcurrentSkipListSet;
////import java.util.regex.Matcher;
////import java.util.regex.Pattern;
////
////import cis5550.flame.FlameContext;
////import cis5550.flame.FlamePair;
////import cis5550.flame.FlamePairRDD;
////import cis5550.flame.FlameRDD;
////import cis5550.flame.FlameContext.RowToString;
////import cis5550.flame.FlamePairRDD.PairToPairIterable;
////import cis5550.flame.FlamePairRDD.PairToStringIterable;
////import cis5550.flame.FlamePairRDD.TwoStringsToString;
////import cis5550.flame.FlameRDD.StringToIterable;
////import cis5550.flame.FlameRDD.StringToPair;
////import cis5550.kvs.KVSClient;
////import cis5550.tools.Hasher;
////import cis5550.tools.URLParser;
////
////public class PageRank {
////	public static List<String> urlExtract(byte[] data) {
////		ArrayList<String> urls = new ArrayList<String>();
////		String htmlText = new String(data);
////		Pattern pattern = Pattern.compile("<([^/][^>]*)>");
////		Matcher matcher = pattern.matcher(htmlText);
////		while (matcher.find()) {
////			String tag = matcher.group(1);
////			String[] tagParts = tag.split("\\s+");
////			String tagType = tagParts[0];
////			if (tagType.toLowerCase().equals("a")) {
////				for (String s: tagParts) {
////					if(s.contains("href=")) {
////						int idx = s.indexOf('=');
////						String url = s.substring(idx + 1);
////						String finalUrl = url.substring(1, url.length() - 1);
////						urls.add(finalUrl);
////					}
////				}
////			}
////		}
////		return urls;
////	}
////	
////	public static List<String> urlNormalize(List<String> urls, String base) {
////		ArrayList<String> normalizedUrls = new ArrayList<String>();
////		String baseUrl = base;
////		for (String url : urls) {
////			String[] urlParts = URLParser.parseURL(url);
////			String protocol = urlParts[0];
////			String hostName = urlParts[1];
////			String port = urlParts[2];
////			String rest = urlParts[3];
////			if (rest.indexOf('#') > -1) {
////				rest = rest.substring(0, rest.indexOf('#'));
////				if (protocol == null && hostName == null && port == null && rest.equals("")) {
////					normalizedUrls.add(baseUrl);
////					continue;
////				}
////			}
////			if (rest.lastIndexOf('.') > -1) {
////				String end = rest.substring(rest.lastIndexOf('.'));
////				if (end.equals(".jpg") || end.equals(".jpeg") || end.equals(".gif") || end.equals(".png") || end.equals(".txt")) {
////					continue;
////				}
////			}
////			if (protocol == null && hostName == null && port == null) {
//////				Either relative or absolute path without hostname
//////				Cuts off part in base URL after last /
////				String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
////				if (rest.length() > 0 && rest.charAt(0) == '/') {
//////					absolute path
////					String[] baseUrlParts = URLParser.parseURL(baseUrl);
////					modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
////					normalizedUrls.add(modifiedBaseUrl+rest);
////					continue;
////				} else {
////					while (rest.length() >= 3 && rest.substring(0, 3).equals("../")) {
////						modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
////						rest = rest.substring(3);
////					}
////					normalizedUrls.add(modifiedBaseUrl+"/"+rest);
////					continue;
////				}
////			}
////			else if (port == null) {
////				String modifiedBaseUrl = "";
////				if (protocol.equals("http")) {
////					modifiedBaseUrl = protocol + "://" + hostName + ":" + "80";
////				} else if (protocol.equals("https")){
////					modifiedBaseUrl = protocol + "://" + hostName + ":" + "443";
////				} else {
////					continue;
////				}
////				normalizedUrls.add(modifiedBaseUrl+rest);
////				continue;
////			}
////			normalizedUrls.add(url);
////		}
////		return normalizedUrls;
////	}
////	
////	public static void run(FlameContext ctx, String[] strArr) {
////		if (strArr.length < 1 || strArr[0] == null) {
////			return;
////		}
////		final double t = Double.parseDouble(strArr[0]);
////		
////		double perc = 100;
////		if (strArr.length >= 2 && strArr[1] != null) {
////			perc = Double.parseDouble(strArr[1]);
////		}
////
////		try {
//////			1. Loading data into PairRDD
////			RowToString lambdaOne = r -> {
////				if(r.get("page") != null && r.get("url") != null) {
////					List<String> extractedUrls = urlExtract(r.getBytes("page"));
////					List<String> normalizedUrls = urlNormalize(extractedUrls, r.get("url"));
////					Set<String> deduplicateUrls = new ConcurrentSkipListSet<String>();
////					for (String s : normalizedUrls) {
////						deduplicateUrls.add(s);
////					}
////					String fullString = "1.0,1.0";
////					for (String url : deduplicateUrls) {
////						if (!r.get("url").equals(url)) {
////							fullString += "," + Hasher.hash(url);
////						}
////					}
////					return Hasher.hash(r.get("url")) + "," + fullString;
////				}
////				return null;
////			};
////			FlameRDD dataLoad = ctx.fromTable("pt-crawl", lambdaOne);
////			
////			StringToPair lambdaTwo = s -> {
////				int commaIdx = s.indexOf(',');
////				String urlHash = s.substring(0, commaIdx);
////				String txt = s.substring(commaIdx+1);
////				FlamePair pair = new FlamePair(urlHash, txt);
////				return pair;
////			};
//////			Maps urlHash to rc,rp,L (L being list of links)
////			FlamePairRDD stateTable = dataLoad.mapToPair(lambdaTwo);
//////			stateTable.saveAsTable("oldStateTable");
////			
////			FlamePairRDD rankCalc;
////			FlamePairRDD aggregatedRank;
////			FlamePairRDD cleanedState;
////			FlameRDD diffs;
////			FlameRDD diffsExceed;
////			
////			while (true) {
//////				2. Finding transfer table
////				PairToPairIterable lambdaThree = p -> {
////					String urlHash = p._1();
////					String pageRankInfo = p._2();
////					String[] rankInfoArr = pageRankInfo.split(",");
////					int n = rankInfoArr.length - 2;
////					double curr = Double.parseDouble(rankInfoArr[0]);
////					double prev = Double.parseDouble(rankInfoArr[1]);
////					double v = 0.85 * curr / n;
////					
////					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
////					for (int i=2; i<rankInfoArr.length; i++) {
////						FlamePair fp = new FlamePair(rankInfoArr[i], String.valueOf(v));
////						pairIterable.add(fp);
////					}
////					pairIterable.add(new FlamePair(urlHash, String.valueOf(0.0)));
////					return pairIterable;
////				};
////				rankCalc = stateTable.flatMapToPair(lambdaThree);
////				
//////				3. Aggregating transfers
////				TwoStringsToString lambdaFour = (s1, s2) -> {
////					double s1Double = Double.parseDouble(s1);
////					double s2Double = Double.parseDouble(s2);
////					return String.valueOf(s1Double + s2Double);
////				};
////				aggregatedRank = rankCalc.foldByKey("0.0", lambdaFour);
////				aggregatedRank.saveAsTable("aggregated");
////				
////				
//////				4. Joining tables, cleaning joined result
////				FlamePairRDD joined = stateTable.join(aggregatedRank);
////				
////				PairToPairIterable lambdaFive = p -> {
////					String urlHash = p._1();
////					String joinedState = p._2();
////					String[] joinedStateSplit = joinedState.split(",");
////					String currRank = joinedStateSplit[0];
////					String prevRank = joinedStateSplit[1];
////					String newRank = joinedStateSplit[joinedStateSplit.length - 1];
////					double newRankFull = Double.parseDouble(newRank) + 0.15;
////					String[] newInfo = new String[joinedStateSplit.length - 1];
////					newInfo[0] = String.valueOf(newRankFull);
////					newInfo[1] = currRank;
////					for (int i = 2; i<newInfo.length; i++) {
////						newInfo[i] = joinedStateSplit[i];
////					}
////					String finalString = "";
////					for (int i = 0; i<newInfo.length; i++) {
////						if (i == newInfo.length - 1) {
////							finalString += newInfo[i];
////						} else {
////							finalString += (newInfo[i] + ",");
////						}
////					}
////					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
////					pairIterable.add(new FlamePair(urlHash, finalString));
////					return pairIterable;
////				};
////				cleanedState = joined.flatMapToPair(lambdaFive);
////				cleanedState.saveAsTable("newStateTable");
////				
////				
//////				Convergence and replacing
////				stateTable = cleanedState;
////				PairToStringIterable lambdaSix = p -> {
////					String urlHash = p._1();
////					String joinedState = p._2();
////					String[] joinedStateSplit = joinedState.split(",");
////					double currRankD = Double.parseDouble(joinedStateSplit[0]);
////					double prevRankD = Double.parseDouble(joinedStateSplit[1]);
////					double diff = Math.abs(currRankD - prevRankD);
////					ArrayList<String> stringIterable = new ArrayList<String>();
////					stringIterable.add(String.valueOf(diff));
////					return stringIterable;
////				};
////				diffs = stateTable.flatMap(lambdaSix);
////				
//////				Goes through each diff and changes it to 1.0 if less than threshold, 0.0 else
////				StringToIterable lambdaSeven = s -> {
////					ArrayList<String> stringIterable = new ArrayList<String>();
////					double diff = Double.parseDouble(s);
////					if (diff <= t) {
////						stringIterable.add("1.0");
////					} else {
////						stringIterable.add("0.0");
////					}
////					return stringIterable;
////				};
////				diffsExceed = diffs.flatMap(lambdaSeven);
////				
//////				Sums over diffsExceed to get number of values of diffs less than threshold
////				TwoStringsToString lambdaEight = (s1, s2) -> {
////					double runSum = Double.parseDouble(s1);
////					double s2D = Double.parseDouble(s2);
////					System.out.println(String.valueOf(runSum + s2D));
////					return String.valueOf(runSum + s2D);
////				};
////				String folded = diffsExceed.fold("0.0", lambdaEight);
////				double lenDiffs = (double) diffs.count();
////				double summed = Double.parseDouble(folded);
////				if (100.0 * (summed/lenDiffs) >= perc) {
////					break;
////				}
////			}
////			
////			PairToPairIterable lambdaNine = p -> {
////				KVSClient kvs = ctx.getKVS();
////				String urlHash = p._1();
////				String joinedState = p._2();
////				String[] joinedStateSplit = joinedState.split(",");
////				String currRank = joinedStateSplit[0];
////				kvs.put("pt-pageranks", urlHash, "rank", currRank);
////				return new ArrayList<FlamePair>();
////			};
////			FlamePairRDD res = stateTable.flatMapToPair(lambdaNine);
////			
////		} catch (Exception e) {
////			e.printStackTrace();
////		}
////	}
////}
//
//
//package cis5550.jobs;
//
//import java.io.IOException;
//import java.net.MalformedURLException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.net.URL;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.LinkedHashSet;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import cis5550.flame.FlameContext;
//import cis5550.flame.FlamePair;
//import cis5550.flame.FlamePairRDDImpl;
//import cis5550.flame.FlameRDDImpl;
//import cis5550.flame.FlameContext.RowToString;
//import cis5550.flame.FlamePairRDD.PairToPairIterable;
//import cis5550.flame.FlamePairRDD.PairToStringIterable;
//import cis5550.flame.FlamePairRDD.TwoStringsToString;
//import cis5550.flame.FlameRDD.StringToBoolean;
//import cis5550.flame.FlameRDD.StringToPair;
//import cis5550.kvs.KVSClient;
//import cis5550.kvs.Row;
//import cis5550.tools.Hasher;
//
//public class PageRank {
//	
//	public static void run(FlameContext context, String[] stringArr) {
//		
//		KVSClient kvs = context.getKVS();
//		String crawlTable = "pt-crawl";
//		
//		System.out.println("crawlTable " + crawlTable);
//		try {
//			System.out.println("inside try");
//			RowToString lambdaFromTable = (Row r) ->{
//				String url = r.get("url");
//				String pageContent = r.get("page");
//				
//				String value = "";
//				Hasher hasher = new Hasher();
//				try {
//					List<String> urls = extractURLs(pageContent, url);
//					LinkedHashSet<String> uniqueLinks = new LinkedHashSet<String>();
//			        for (String link : urls) {
//			        	if(!link.equals("")) uniqueLinks.add(hasher.hash(link));
////			        	if(!link.equals("")) uniqueLinks.add(link);
//			        }
//					String urlString = String.join(",", uniqueLinks);
//					value = "1.0,1.0," + urlString;
//				} catch (MalformedURLException | URISyntaxException e) {
//					e.printStackTrace();
//				}
//				
//				return hasher.hash(url) + "," + value;
////				return url + "," + value;
//			};
//			FlameRDDImpl rdd = (FlameRDDImpl) context.fromTable(crawlTable, lambdaFromTable);
////			rdd.saveAsTable("pt-pagerank");
//			
//			StringToPair lambdaMapToPair = (String s) ->{
//				String[] parts = s.split(",", 2);
////				System.out.println("lambdaMapToPair " + parts[0] + "       " + parts[1]);
//				return new FlamePair(parts[0], parts[1]);
//			};
//			
//			FlamePairRDDImpl oldStateTable = (FlamePairRDDImpl) rdd.mapToPair(lambdaMapToPair);
////			oldStateTable.saveAsTable("pt-pagerank");
////		    pairRDD.saveAsTable("pt-pagerank");
//			while(true) {
//				System.out.println("NEW ITERATION");
//				FlamePairRDDImpl oldWeightedPairsRDD = computeTransfer(oldStateTable);
//			
//			//state table
//				FlamePairRDDImpl aggregatedWeightedPairsRDD = aggregateTransfer(oldWeightedPairsRDD);
//			//Aggregate weights
//				FlamePairRDDImpl joined = (FlamePairRDDImpl) aggregatedWeightedPairsRDD.join(oldStateTable);
////			joined.saveAsTable("joinedTable");
//			
//			//new state table
//				FlamePairRDDImpl updatedWeightedPairsRDD = updateTable(joined);
//				
//				oldStateTable = updatedWeightedPairsRDD;
//				
////				oldStateTable.saveAsTable("updatedTable");
//				PairToStringIterable lambdaFlatMap = (FlamePair pair) -> {
////					String key = pair._1();
////					System.out.println("key " + key);
//					String value = pair._2();
////					System.out.println("value " + value);
//					
//					String[] parts = value.split(",", 3);
//					double currentRank = Double.parseDouble(parts[0]);
//					double oldRank = Double.parseDouble(parts[1]);
////					double r_p = Double.parseDouble(parts[2]);
////					String urlList = parts[2];
//					double diff = Math.abs(currentRank - oldRank);
//				
//					ArrayList<String> weightDiff = new ArrayList<String>();
//					weightDiff.add(String.valueOf(diff));
//			        
//			        Iterable<String> iterator = weightDiff;
//			        return iterator;
//				};
//				
//				FlameRDDImpl weightDiffRDD = (FlameRDDImpl) oldStateTable.flatMap(lambdaFlatMap);
////				weightDiffRDD.saveAsTable("diff");
//				
//				if(stringArr.length == 1) {
//					TwoStringsToString lambdaFold = (String a, String b) -> { 
//						double aVal = Double.parseDouble(a);
//						double bVal = Double.parseDouble(b);
//	//					System.out.println("value in newWeightedPairsRDD: " + (aVal+bVal));
//						double max = Math.max(aVal, bVal);
//						return String.valueOf(max);
//					};
//					String max = weightDiffRDD.fold("0.0", lambdaFold);
//					double maxDiff = Double.parseDouble(max);
//		
//					for(int i = 0; i < stringArr.length; i++) {
//						System.out.println(stringArr[i]);
//					}
//					
//					double convergenceThreshold = Double.parseDouble(stringArr[0]);
//					if(maxDiff < convergenceThreshold) break;
////					
//				} else {
//					double convergenceThreshold = Double.parseDouble(stringArr[0]);
//					double proportion = Double.parseDouble(stringArr[1]);
//					System.out.println("proportion " + proportion);
//				
//					StringToBoolean lambdaFilter = (String s) -> { 
//						System.out.println("s " + s);
//						boolean result = Double.parseDouble(s) < convergenceThreshold;
//						return result;
//					};
//					FlameRDDImpl filteredRDD = (FlameRDDImpl) weightDiffRDD.filter(lambdaFilter);
//					int initialSize = weightDiffRDD.count();
//					int newSize = filteredRDD.count();
//					System.out.println("size : " + newSize);
//					System.out.println("proportion/100 : " + proportion/100);
//					System.out.println("newSize/(initialSize*1.0) : " + (newSize/(initialSize*1.0)));
//					if(newSize/(initialSize*1.0) >= proportion/100) break;
//				}
////				//if(maxDiff < convergenceThreshold) break;
//			}
//			
////			System.out.println("before flatMap");
//////			oldStateTable.saveAsTable("updatedTable");
//			PairToPairIterable lambdaFlatMapToPair = (FlamePair pair) -> {
//				KVSClient k = context.getKVS();
//				
//				String key = pair._1();
//				System.out.println("key " + key);
//				String value = pair._2();
//				System.out.println("value " + value);
//				
//				String[] parts = value.split(",", 3);
//				double rank = Double.parseDouble(parts[0]);
//				System.out.println("rank " + rank);
//				
//				k.put("pt-pageranks", key, "rank", String.valueOf(rank));
//				
//		        ArrayList<FlamePair> list = new ArrayList<FlamePair>();
//		       
//		        Iterable<FlamePair> iterator = list;
////		        return iterator;
//		        return Collections.emptyList();
//			};
//			oldStateTable.flatMapToPair(lambdaFlatMapToPair);
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}	
//		
//	}
//	
//	public static List<String> extractURLs(String page, String base) 
//			throws MalformedURLException, URISyntaxException {
//		List<String> containedUrls = new ArrayList<>();
//		Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", 
//				Pattern.CASE_INSENSITIVE);
//        Matcher matcher = pattern.matcher(page);
//        
//        while(matcher.find()) {
//        	//retrieves part of pattern that matches the first capturing group
//        	String url = matcher.group(1);
//            if (url != null && !url.isEmpty()) {
//            	String normalizedURL = normalizeURL(url, base);
//            	if(normalizedURL.length() != 0) {
//            		containedUrls.add(normalizedURL);
//            	}
//            }
//        }
//        
//        return containedUrls;
//	}
//	
//	public static String normalizeURL(String malformed, String base) 
//			throws MalformedURLException, URISyntaxException {
//		
//		
//		URI uriBase = new URI(base);
//		URL urlBase = uriBase.toURL();
//		
//		String withoutHash = malformed.split("#", 2)[0];
//		URL urlCombined = new URL(urlBase, withoutHash);
//		
//		StringBuilder result = new StringBuilder();
//		result.append(urlCombined.getProtocol()).append("://").append(urlCombined.getHost());
//		
//		int port = urlCombined.getPort();
//		if(port == -1) {
//			if(urlCombined.getProtocol().equals("http")) port = 80;
//			else if(urlCombined.getProtocol().equals("https")) port = 443;
//		}
//		result.append(":").append(port).append(urlCombined.getFile());
//		String resultString = result.toString();
//		
//		if(!(resultString.endsWith(".jpg") || resultString.endsWith(".jpeg") || 
//				resultString.endsWith(".png") ||
//				resultString.endsWith(".gif") || 
//				resultString.endsWith(".txt"))) {
//			return resultString;
//		}
//        return "";
//	}
//
//	public static FlamePairRDDImpl computeTransfer(FlamePairRDDImpl oldStateTable){
//		 PairToPairIterable lambdaFlatMapToPair = (FlamePair pair) -> {
//				String key = pair._1();
////				System.out.println("key " + key);
//				String value = pair._2();
////				System.out.println("value " + value);
//				
//				String[] parts = value.split(",", 3);
//				double r_c = Double.parseDouble(parts[0]);
////				System.out.println("r_C " + r_c);
//				
//				String urlString = parts[2];
//				String[] urls = urlString.split(",");
//				int n = urls.length;
////				System.out.println("n " + n);
//				
//				Hasher hasher = new Hasher();
//				
//		        ArrayList<FlamePair> urlWeights = new ArrayList<FlamePair>();
//		        
//		        for(String url: urls) {
//		        	if(url.isEmpty()) continue;
//	        		double v = 0.85*r_c/n;
//	        		FlamePair p = new FlamePair(url, String.valueOf(v));
//		        	
//		        	urlWeights.add(p);
//		        }
//
//	        	FlamePair p = new FlamePair(key, String.valueOf(0.0));
//	        	urlWeights.add(p);
//		        
//		        Iterable<FlamePair> iterator = urlWeights;
////		        System.out.println(iterator);
//		        return iterator;
//			};
//			
//			FlamePairRDDImpl oldWeightedPairsRDD = null;
//			try {
//				oldWeightedPairsRDD = (FlamePairRDDImpl) oldStateTable.flatMapToPair(lambdaFlatMapToPair);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			return oldWeightedPairsRDD;
////			oldWeightedPairsRDD.saveAsTable("transferTable");
//	}
//	
//	public static FlamePairRDDImpl aggregateTransfer(FlamePairRDDImpl oldWeightedPairsRDD) {
//		TwoStringsToString lambdaFoldByKey = (String a, String b) -> { 
//			double aVal = Double.parseDouble(a);
//			double bVal = Double.parseDouble(b);
////			System.out.println("value in newWeightedPairsRDD: " + (aVal+bVal));
//			return String.valueOf(aVal+bVal);
//		};
//		FlamePairRDDImpl aggregatedWeightedPairsRDD = null;
//		
//		try {
//			aggregatedWeightedPairsRDD = (FlamePairRDDImpl) oldWeightedPairsRDD.foldByKey("0.0", lambdaFoldByKey);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return aggregatedWeightedPairsRDD;
////		newWeightedPairsRDD.saveAsTable("aggregatedTable");
//		
//	}
//	
//	public static FlamePairRDDImpl updateTable(FlamePairRDDImpl joined) {
//		PairToPairIterable lambdaFlatMapToPair2 = (FlamePair pair) -> {
//			String key = pair._1();
////			System.out.println("key " + key);
//			String value = pair._2();
////			System.out.println("value " + value);
//			
//			String[] parts = value.split(",", 4);
//			double newWeight = Double.parseDouble(parts[0]);
//			double r_c = Double.parseDouble(parts[1]);
//			double r_p = Double.parseDouble(parts[2]);
//			String urlList = parts[3];
////			System.out.println("r_C " + r_c);
//			
//			r_p = r_c;
//			r_c = newWeight + 0.15;
////			newWeight += 0.15;
//			String newValue = String.valueOf(r_c) + "," + String.valueOf(r_p) + "," + urlList;
//			
//			ArrayList<FlamePair> updatedUrlWeights = new ArrayList<FlamePair>();
//			FlamePair p = new FlamePair(key, newValue);
//			updatedUrlWeights.add(p);
//	        
//	        Iterable<FlamePair> iterator = updatedUrlWeights;
////	        System.out.println(iterator);
//	        return iterator;
//		};
//		
//		FlamePairRDDImpl updatedWeightedPairsRDD = null;
//		try {
//			updatedWeightedPairsRDD = (FlamePairRDDImpl) joined.flatMapToPair(lambdaFlatMapToPair2);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
////		updatedWeightedPairsRDD.saveAsTable("newStateTable");
//		return updatedWeightedPairsRDD;
//	}
//}
//
//



package cis5550.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	public static List<String> urlExtract(byte[] data) {
		ArrayList<String> urls = new ArrayList<String>();
		String htmlText = new String(data);
		Pattern pattern = Pattern.compile("<([^/][^>]*)>");
		Matcher matcher = pattern.matcher(htmlText);
		int urlCount = 0;
		while (matcher.find()) {
			String tag = matcher.group(1);
			String[] tagParts = tag.split("\\s+");
			String tagType = tagParts[0];
			if (tagType.toLowerCase().equals("a")) {
				for (String s: tagParts) {
					if(s.contains("href=")) {
						int idx = s.indexOf('=');
						String url = s.substring(idx + 1);
						if (url.length() > 1) {
							String finalUrl = url.substring(1, url.length() - 1);
							urls.add(finalUrl);
							// urlCount ++;
							// if (urlCount >= 70) {
							// 	return urls;
							// }
						}
					}
				}
			}
		}
		return urls;
	}
	
	public static List<String> urlNormalize(List<String> urls, String base) {
		ArrayList<String> normalizedUrls = new ArrayList<String>();
		String baseUrl = base;
		for (String url : urls) {
			String[] urlParts = URLParser.parseURL(url);
			String protocol = urlParts[0];
			String hostName = urlParts[1];
			String port = urlParts[2];
			String rest = urlParts[3];
			if (rest.indexOf('#') > -1) {
				rest = rest.substring(0, rest.indexOf('#'));
				if (protocol == null && hostName == null && port == null && rest.equals("")) {
					normalizedUrls.add(baseUrl);
					continue;
				}
			}
			if (rest.lastIndexOf('.') > -1) {
				String end = rest.substring(rest.lastIndexOf('.'));
				if (end.equals(".jpg") || end.equals(".jpeg") || end.equals(".gif") || end.equals(".png") || end.equals(".txt")) {
					continue;
				}
			}
			if (protocol == null && hostName == null && port == null) {
//				Either relative or absolute path without hostname
//				Cuts off part in base URL after last /
				String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
				if (rest.length() == 0 || rest.charAt(0) == '/') {
//					absolute path
					String[] baseUrlParts = URLParser.parseURL(baseUrl);
					modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
					normalizedUrls.add(modifiedBaseUrl+rest);
					continue;
				} else {
					while (rest.length() >= 3 && rest.substring(0, 3).equals("../")) {
						modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
						rest = rest.substring(3);
					}
					normalizedUrls.add(modifiedBaseUrl+"/"+rest);
					continue;
				}
			}
			else if (port == null) {
				String modifiedBaseUrl = "";
				if (protocol.equals("http")) {
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "80";
				} else if (protocol.equals("https")){
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "443";
				} else {
					continue;
				}
				normalizedUrls.add(modifiedBaseUrl+rest);
				continue;
			}
			normalizedUrls.add(url);
		}
		return normalizedUrls;
	}
	
	public static void run(FlameContext ctx, String[] strArr) {
		if (strArr.length < 1 || strArr[0] == null) {
			return;
		}
		final double t = Double.parseDouble(strArr[0]);
		
		double perc = 100;
		if (strArr.length >= 2 && strArr[1] != null) {
			perc = Double.parseDouble(strArr[1]);
		}

		try {
//			1. Loading data into PairRDD
			RowToString lambdaOne = r -> {
				if(r.get("page") != null && r.get("url") != null) {
					List<String> extractedUrls = urlExtract(r.getBytes("page"));
					List<String> normalizedUrls = urlNormalize(extractedUrls, r.get("url"));
					Set<String> deduplicateUrls = new ConcurrentSkipListSet<String>();
					for (String s : normalizedUrls) {
						deduplicateUrls.add(s);
					}
					String fullString = "1.0,1.0";
					for (String url : deduplicateUrls) {
						if (!r.get("url").equals(url)) {
							fullString += "," + Hasher.hash(url);
						}
					}
					return Hasher.hash(r.get("url")) + "," + fullString;
				}
				return null;
			};
			FlameRDD dataLoad = ctx.fromTable("pt-crawl", lambdaOne);
			
			StringToPair lambdaTwo = s -> {
				int commaIdx = s.indexOf(',');
				String urlHash = s.substring(0, commaIdx);
				String txt = s.substring(commaIdx+1);
				FlamePair pair = new FlamePair(urlHash, txt);
				return pair;
			};
//			Maps urlHash to rc,rp,L (L being list of links)
			FlamePairRDD stateTable = dataLoad.mapToPair(lambdaTwo);
			dataLoad.destroy();
//			stateTable.saveAsTable("oldStateTable");
			
			FlamePairRDD rankCalc;
			FlamePairRDD aggregatedRank;
			FlamePairRDD cleanedState;
			FlameRDD diffs;
			FlameRDD diffsExceed;
			
			int iter = 0;
			while (true) {
				System.out.println("Iter: " + iter++);
//				2. Finding transfer table
				PairToPairIterable lambdaThree = p -> {
					String urlHash = p._1();
					String pageRankInfo = p._2();
					String[] rankInfoArr = pageRankInfo.split(",");
					int n = rankInfoArr.length - 2;
					double curr = Double.parseDouble(rankInfoArr[0]);
					double prev = Double.parseDouble(rankInfoArr[1]);
					double v = 0.85 * curr / n;
					
					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
					for (int i=2; i<rankInfoArr.length; i++) {
						FlamePair fp = new FlamePair(rankInfoArr[i], String.valueOf(v));
						pairIterable.add(fp);
					}
					pairIterable.add(new FlamePair(urlHash, String.valueOf(0.0)));
					return pairIterable;
				};
				rankCalc = stateTable.flatMapToPair(lambdaThree);
				
//				3. Aggregating transfers
				TwoStringsToString lambdaFour = (s1, s2) -> {
					double s1Double = Double.parseDouble(s1);
					double s2Double = Double.parseDouble(s2);
					return String.valueOf(s1Double + s2Double);
				};
				aggregatedRank = rankCalc.foldByKey("0.0", lambdaFour);
				rankCalc.destroy();
//				aggregatedRank.saveAsTable("aggregated");
				
				
//				4. Joining tables, cleaning joined result
				FlamePairRDD joined = stateTable.join(aggregatedRank);
				
				PairToPairIterable lambdaFive = p -> {
					String urlHash = p._1();
					String joinedState = p._2();
					String[] joinedStateSplit = joinedState.split(",");
					String currRank = joinedStateSplit[0];
					String prevRank = joinedStateSplit[1];
					String newRank = joinedStateSplit[joinedStateSplit.length - 1];
					double newRankFull = Double.parseDouble(newRank) + 0.15;
					String[] newInfo = new String[joinedStateSplit.length - 1];
					newInfo[0] = String.valueOf(newRankFull);
					newInfo[1] = currRank;
					for (int i = 2; i<newInfo.length; i++) {
						newInfo[i] = joinedStateSplit[i];
					}
					String finalString = "";
					for (int i = 0; i<newInfo.length; i++) {
						if (i == newInfo.length - 1) {
							finalString += newInfo[i];
						} else {
							finalString += (newInfo[i] + ",");
						}
					}
					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
					pairIterable.add(new FlamePair(urlHash, finalString));
					return pairIterable;
				};
				cleanedState = joined.flatMapToPair(lambdaFive);
				joined.destroy();
//				cleanedState.saveAsTable("newStateTable");
				
				
//				Convergence and replacing
				stateTable = cleanedState;
				PairToStringIterable lambdaSix = p -> {
					String urlHash = p._1();
					String joinedState = p._2();
					String[] joinedStateSplit = joinedState.split(",");
					double currRankD = Double.parseDouble(joinedStateSplit[0]);
					double prevRankD = Double.parseDouble(joinedStateSplit[1]);
					double diff = Math.abs(currRankD - prevRankD);
					ArrayList<String> stringIterable = new ArrayList<String>();
					stringIterable.add(String.valueOf(diff));
					return stringIterable;
				};
				diffs = stateTable.flatMap(lambdaSix);
				
//				Goes through each diff and changes it to 1.0 if less than threshold, 0.0 else
				StringToIterable lambdaSeven = s -> {
					ArrayList<String> stringIterable = new ArrayList<String>();
					double diff = Double.parseDouble(s);
					if (diff <= t) {
						stringIterable.add("1.0");
					} else {
						stringIterable.add("0.0");
					}
					return stringIterable;
				};
				diffsExceed = diffs.flatMap(lambdaSeven);
				diffs.destroy();
				
//				Sums over diffsExceed to get number of values of diffs less than threshold
				TwoStringsToString lambdaEight = (s1, s2) -> {
					double runSum = Double.parseDouble(s1);
					double s2D = Double.parseDouble(s2);
//					System.out.println(String.valueOf(runSum + s2D));
					return String.valueOf(runSum + s2D);
				};
				String folded = diffsExceed.fold("0.0", lambdaEight);
				double lenDiffs = (double) diffs.count();
				double summed = Double.parseDouble(folded);
				if (100.0 * (summed/lenDiffs) >= perc) {
					break;
				}
			}
			
			PairToPairIterable lambdaNine = p -> {
				KVSClient kvs = ctx.getKVS();
				String urlHash = p._1();
				String joinedState = p._2();
				String[] joinedStateSplit = joinedState.split(",");
				String currRank = joinedStateSplit[0];
				kvs.put("pt-pageranks", urlHash, "rank", currRank);
				return new ArrayList<FlamePair>();
			};
			FlamePairRDD res = stateTable.flatMapToPair(lambdaNine);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
