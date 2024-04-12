package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;

public class SocketProcessor extends Thread{
//	private static final Logger logger = Logger.getLogger(Server.class);
	
	protected BlockingQueue<Socket> socketQueue;
	protected int port;
	protected String directory;
	
	public SocketProcessor(BlockingQueue<Socket> socketQueue, int port, String directory) {
		this.socketQueue = socketQueue;
		this.port = port;
		this.directory = directory;
	}
	
	public void run() {
		while (true) {
			Socket sock = null;
			try {
				sock = socketQueue.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			System.out.println("Connection from: "+sock.getRemoteSocketAddress());
//			logger.info("Incoming connection from"+sock.getRemoteSocketAddress());
			
			InputStream ipr = null;
			try {
				ipr = sock.getInputStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			while (true) {
//				READING HEADER
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				
				int curr = 0;
				byte[] agg = null;
				try {
					while((curr = ipr.read()) != -1) {
						bo.write(curr);
						agg = bo.toByteArray();
						
						if (agg.length > 4 
							&& agg[agg.length - 1] == 10 
							&& agg[agg.length - 2] == 13
							&& agg[agg.length - 3] == 10 
							&& agg[agg.length - 4] == 13) 
						{
							break;
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
//				CHECKS FOR CLIENT DISCONNECT
				if (curr == -1) {
					break;
				}
				
				byte[] headerBytes = agg;
				ArrayList<String> headerLines = new ArrayList<String>();
				BufferedReader headerReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes)));
				String current = "";
				try {
					while(((current = headerReader.readLine()) != null) && current.length() > 0) {
//						System.out.println(current);
						headerLines.add(current);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
								
//				READ BODY
				int contentLength = Server.contentLength(headerLines);
				byte[] bodyBytes = null;
				ByteArrayOutputStream boBody = new ByteArrayOutputStream();
				int currBody = 0;
				int idx = 0;
				try {
					while((idx < contentLength && (currBody = ipr.read()) != -1)) {
						boBody.write(currBody);
						idx++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				bodyBytes = boBody.toByteArray();
				
//				SEND RESPONSE
				OutputStream opstream = null;
				try {
					opstream = sock.getOutputStream();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer = new PrintWriter(opstream); 
				
				int statusCode = Server.headerCheck(headerLines);
				
				if (statusCode != 200 || directory.contains("..")) {
					String response = "";
					if (statusCode == 400) {
						response = "HTTP/1.1 400 Bad Request\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "400 Bad Request".length() + "\r\n\r\n"
								+ "400 Bad Request\n";
					} else if (statusCode == 405) {
						response = "HTTP/1.1 405 Method Not Allowed\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "405 Method Not Allowed".length() + "\r\n\r\n"
								+ "405 Method Not Allowed\n";
					} else if (statusCode == 501) {
						response = "HTTP/1.1 501 Not Implemented\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "501 Not Implemented".length() + "\r\n\r\n"
								+ "501 Not Implemented\n";
					} else if (statusCode == 505) {
						response = "HTTP/1.1 505 HTTP Version Not Supported\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "505 HTTP Version Not Supported".length() + "\r\n\r\n"
								+ "505 HTTP Version Not Supported\n";
					} else if (statusCode == 403 || directory.contains("..")) {
						response = "HTTP/1.1 403 Forbidden\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "403 Forbidden".length() + "\r\n\r\n"
								+ "403 Forbidden\n";
					}
					writer.write(response);
					writer.flush();
				}
				
				else {
					
//					READ FILE
					try {
						String filepath = Server.filePath(headerLines.get(0));
			//			System.out.println(filepath);
//						System.out.println(directory+filepath);
						File f = new File(directory+filepath);
						FileInputStream fileip = new FileInputStream(f);
						
						String modifiedDate = Server.modifiedSince(headerLines);
						
						if(modifiedDate.equals("NONE")) {
//							Don't have to check for modified date
//							SEND RESPONSE
							String response = "HTTP/1.1 200 OK\r\n"
											+ "Content-Type:" + Server.contentType(headerLines) + "\r\n"
											+ "Server: localhost\r\n"
											+ "Content-Length: " + Long.toString(f.length()) + "\r\n\r\n";
							writer.write(response);
							writer.flush();
							
//							DON'T SEND BODY IF NOT GET METHOD (HEAD REQUEST)
							if(Server.getOrHead(headerLines)) {
								DataOutputStream fileStream = new DataOutputStream(opstream);
								byte[] buffer = new byte[(int) f.length()];
								while (fileip.read(buffer) != -1) {
									fileStream.write(buffer, 0, buffer.length);
									fileStream.flush();
							    }
								fileip.close();
							} 
						} else {
//							Need to check modified date
							long fileModified = f.lastModified();
							SimpleDateFormat formatter = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
							java.util.Date requestedModified = formatter.parse(modifiedDate);
							long requestedModifiedTime = requestedModified.getTime();
							if (requestedModifiedTime < fileModified) {
								String response = "HTTP/1.1 200 OK\r\n"
										+ "Content-Type:" + Server.contentType(headerLines) + "\r\n"
										+ "Server: localhost\r\n"
										+ "Content-Length: " + Long.toString(f.length()) + "\r\n\r\n";
								writer.write(response);
								writer.flush();
								
		//						DON'T SEND BODY IF NOT GET METHOD (HEAD REQUEST)
								if(Server.getOrHead(headerLines)) {
									DataOutputStream fileStream = new DataOutputStream(opstream);
									byte[] buffer = new byte[(int) f.length()];
									while (fileip.read(buffer) != -1) {
										fileStream.write(buffer, 0, buffer.length);
										fileStream.flush();
								    }
									fileip.close();
								} 
							} else {
//								Not modified since, so error
								String response = "HTTP/1.1 304 Not Modified\r\n"
										+ "Content-Type: text/plain\r\n"
										+ "Server: localhost\r\n"
										+ "Content-Length: " + "304 Not Modified".length() + "\r\n\r\n"
										+ "304 Not Modified\n";
								writer.write(response);
								writer.flush();
							}
						}
						
//					System.out.println(Arrays.toString(buffer));
					} catch (FileNotFoundException e) {
						String response = "HTTP/1.1 404 Not Found\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "404 Not Found".length() + "\r\n\r\n"
								+ "404 Not Found\n";
						writer.write(response);
						writer.flush();
					} catch (SecurityException e) {
						String response = "HTTP/1.1 403 Forbidden\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "403 Forbidden".length() + "\r\n\r\n"
								+ "403 Forbidden\n";
						writer.write(response);
						writer.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
//				ALSO CHECKS FOR DISCONNECT
				if (Server.closeConnection(headerLines)) {
					break;
				}
			}
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
