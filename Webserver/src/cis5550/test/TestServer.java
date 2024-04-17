package cis5550.test;
import static cis5550.webserver.Server.*;

public class TestServer {
	public static void main(String args[]) throws Exception {
      port(8080);
	    securePort(443);
	    get("/", (req,res) -> { return "Hello World - this is Kevin Zhang"; });
	}
}
