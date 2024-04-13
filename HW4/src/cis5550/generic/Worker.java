package cis5550.generic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.kvs.Row;
import cis5550.tools.HTTP;

public class Worker {
	public static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<String, Map<String, Row>>();
	
	public static void startPingThread(String coordIpPort, String workerPort, String id) {
		Thread pingThread = new Thread(() -> {
			while (true) {				
				String url = "http://"+coordIpPort+"/ping?id="+id+"&port="+workerPort;
				try {
					HTTP.doRequest("GET", url, null);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		pingThread.start();
	}
}
