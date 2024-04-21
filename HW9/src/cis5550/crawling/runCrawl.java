package cis5550.crawling;


public class runCrawl {
	
	public static void main(String args[]) throws Exception {
		System.out.println("Running the crawler");
		String arg[] = new String[] { "https://www.cnn.com/" };
		String output = FlameSubmit.submit("localhost:9000", "crawler.jar", "cis5550.jobs.Crawler", arg);
		if (output != null) {
			System.out.println("Done");
		}
  }
}
