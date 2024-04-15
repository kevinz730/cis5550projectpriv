package cis5550.Webserver.test;

import static cis5550.Webserver.webserver.Server.*;

public class TestServer {
	public static void main(String args[]) throws Exception {
	    securePort(443);
	    get("/", (req,res) -> { return "Hello World - this is Kevin Zhang"; });
	}
}