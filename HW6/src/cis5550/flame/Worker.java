package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	public static String[] decodeQueryParams (Request request) {
        String[] result = new String[7];
        result[0] = request.queryParams("input");
        result[1] = request.queryParams("output");
        result[2] = request.queryParams("coordinator");
        result[3] = request.queryParams("low");
        result[4] = request.queryParams("high");
        result[5] = request.queryParams("zeroElement");
        result[6] = request.queryParams("prob");
        return result;
	}
	
	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flatMap", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Iterable<String> res = lambda.op(r.get("value"));
        	if (res != null) {
        		Iterator<String> it = res.iterator();
        		while (it.hasNext()) {
        			String val = it.next();
    				try {
    					kvs.put(outputTable, UUID.randomUUID().toString(), "value", val);
    				} catch (Exception e) {
    					e.printStackTrace();
    				}
        		}
        	}
        }
        return "OK";
      });
	
	post("/rdd/mapToPair", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	FlamePair res = lambda.op(r.get("value"));
        	if (res != null) {
            	String k = res._1();
            	String v = res._2();
				try {
					kvs.put(outputTable, k, r.key(), v);
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        }
        return "OK";
      });
	
	post("/rdd/foldByKey", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        String zeroElement = URLDecoder.decode(queryParamsDecoded[5], "UTF-8");
        FlamePairRDD.TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String acc = zeroElement;
        	Set<String> columns = r.columns();
			for (String c : columns) {
				String v = r.get(c);
				String newAcc = lambda.op(acc, v);
				acc = newAcc;
			}
			try {
				kvs.put(outputTable, r.key(), UUID.randomUUID().toString(), acc);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        return "OK";
      });
	
	post("/rdd/sampling", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        String zeroElement = URLDecoder.decode(queryParamsDecoded[5], "UTF-8");
        double prob = Double.parseDouble(queryParamsDecoded[6]);
        FlamePairRDD.TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Random rand = new Random();
        	double generatedProb = rand.nextDouble();
        	if (generatedProb <= prob) {
        		try {
    				kvs.put(outputTable, r.key(), "value", r.get("value"));
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
        	}
        }
        return "OK";
      });
	
	}	
}
