package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;

public class Server {
		
//	private static final Logger logger = Logger.getLogger(Server.class);
	
	protected static final int NUM_WORKERS = 100;
	
	protected static int contentLength(ArrayList<String> headerLines) {
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("content-length")) {
				String length = s.substring(16);
				return Integer.parseInt(length);
			}
		}
		return 0;
	}
	
	protected static boolean closeConnection(ArrayList<String> headerLines) {
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("connection")) {
				String status = s.substring(12);
				return status.equals("close");
			}
		}
		return false;
	}
	
	protected static String filePath(String headerLine) {
		String[] words = headerLine.split(" ");
		return words[1];
	}
	
	protected static int headerCheck(ArrayList<String> headerLines) {
		String[] firstLineArray = headerLines.get(0).split(" ");
		String firstLine = headerLines.get(0);
//		missing method, URL, protocol, or Host
		if (firstLineArray.length != 3 || headerLines.size() < 2) {
//			System.out.println(firstLine.length);
			return 400;
		}
		if (firstLineArray[1].contains("..")) {
			return 403;
		}
		if (!firstLine.contains("HTTP/")) {
			return 400;
		}
		if (firstLine.contains("POST") || firstLine.contains("PUT")) {
			return 405;
		}
		if (!(firstLine.contains("GET") || firstLine.contains("HEAD") || firstLine.contains("POST") || firstLine.contains("PUT"))) {
			return 501;
		}
		if (!firstLine.contains("HTTP/1.1")) {
			return 505;
		}
		
		boolean hostExist = false;
		for (String s : headerLines) {
			if (s.toLowerCase().contains("host:")) {
				hostExist = true;
			}
		}
		if (!hostExist) return 400;
		
		return 200;
	}
	
	protected static boolean getOrHead(ArrayList<String> headerLines) {
//		true for get, false for head
		if (headerLines.get(0).contains("GET")) {
			return true;
		} else return false;
	}
	
	protected static String contentType(ArrayList<String> headerLines) {
//		true for get, false for head
		String[] firstLineArray = headerLines.get(0).split(" ");
		if (firstLineArray[1].contains(".jpg") || firstLineArray[1].contains(".jpeg")) {
			return "image/jpeg";
		} else if (firstLineArray[1].contains(".txt")) {
			return "text/plain";
		} else if (firstLineArray[1].contains(".html")) {
			return "text/html";
		} else {
			return "application/octet-stream";
		}
	}
	
	protected static String modifiedSince(ArrayList<String> headerLines) {
//		true for get, false for head
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("if-modified-since")) {
				String date = s.substring(19);
				return date;
			}
		}
		return "NONE";
	}
	
	public static void main(String[] args) throws Exception {
		
		BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>();
		
				
		if(args.length != 2) {
			System.out.println("Written by Kevin Zhang");
			return;
		}
		
		int port = Integer.parseInt(args[0]);
		String directory = args[1];
		
		ServerSocket ssock = new ServerSocket(port);
		
		
		for (int i = 0; i < NUM_WORKERS; i++) { 
			SocketProcessor thread = new SocketProcessor(socketQueue, port, directory);
			thread.start();
		}
		
		while(true) {
			Socket sock = ssock.accept();
			
//			ADD TO BLOCKING QUEUE
			socketQueue.add(sock);
		}

		
	}
}
