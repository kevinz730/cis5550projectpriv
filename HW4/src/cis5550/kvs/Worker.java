package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {
	public static void putRow(String t, Row r) {
		tables.get(t).put(r.key(),r);
	}
	
	public static Row getRow(String t, String k) {
		return tables.get(t).get(k);
	}
	
	public static void main (String[] args) {
        String workerPort = args[0];
        String directory = args[1];
        String coordIpPort = args[2];
        
        String id = "";
                
        try {
        	File f = new File(directory+"/id");
        	if (f.exists()) {
        		FileInputStream fileip = new FileInputStream(f);
            	byte[] buffer = new byte[(int) f.length()];
    			fileip.read(buffer);
    			id = new String(buffer);
    			fileip.close();
        	}
        	else {
        		StringBuilder randomString = new StringBuilder();
				Random random = new Random();
		        for (int i = 0; i < 5; i++) {
		            int randomNumber = random.nextInt(26) + 97;
		            char randomChar = (char) randomNumber;
		            randomString.append(randomChar);
		        }
		        id = randomString.toString();
		        FileOutputStream outputStream = new FileOutputStream(f);
		        byte[] idBytes = id.getBytes();
		        outputStream.write(idBytes);
		        outputStream.close();
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        port(Integer.parseInt(workerPort));
        startPingThread(coordIpPort, workerPort, id);
        
        put("/data/:t/:r/:c", (req,res) -> { 
        	String table = req.params("t");
        	String row = req.params("r");
        	String column = req.params("c");
        	
        	if(req.queryParams("ifcolumn") != null && req.queryParams("equals") != null) {
        		String colName = req.queryParams("ifcolumn");
        		String colValue = req.queryParams("equals");
        		if(!tables.containsKey(table)) {
        			return "FAIL";
        		} else if (!tables.get(table).containsKey(row)) {
        			return "FAIL";
        		} else if (tables.get(table).get(row).get(colName) == null) {
        			return "FAIL";
        		} else if (tables.get(table).get(row).get(colName) != colValue) {
        			return "FAIL";
        		}
        	}
        	
			if(tables.containsKey(table)) {
				Map<String, Row> existing = tables.get(table);
				if(existing.containsKey(row)) {
					byte[] data = req.bodyAsBytes();
					Row currRow = existing.get(row);
					currRow.put(column, data);
					putRow(table, currRow);
				} else {
					byte[] data = req.bodyAsBytes();
					Row newRow = new Row(row);
					newRow.put(column, data);
					putRow(table, newRow);
				}
			} else {
				tables.put(table, new ConcurrentHashMap<String, Row>());
				Map<String, Row> newTable = tables.get(table);
				byte[] data = req.bodyAsBytes();
				Row newRow = new Row(row);
				newRow.put(column, data);
				putRow(table, newRow);
			}
			
			return "OK";
		});
        
        get("/data/:t/:r/:c", (req,res) -> { 
        	String table = req.params("t");
        	String row = req.params("r");
        	String column = req.params("c");
        	
        	if (tables.containsKey(table) && tables.get(table).containsKey(row) && tables.get(table).get(row).get(column) != null) {
        		byte[] data = getRow(table, row).getBytes(column);
        		res.bodyAsBytes(data);
        	} else {
        		res.status(404, "Not Found");
        	}
        	return null;
		});
    }
}
