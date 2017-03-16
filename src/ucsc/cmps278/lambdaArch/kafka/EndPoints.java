package ucsc.cmps278.lambdaArch.kafka;

import java.util.HashMap;

/**
 *	The logic to read this end points configuration from a file 
 *	and populating this map can done later. Keep it in-memory for now.
 */
public class EndPoints {
	private HashMap<String, String> routes =  new HashMap<>();

	// Ctor
	public EndPoints() {
		// Later on read external configuration files here
		routes.put("Route_704", "http://api.metro.net/agencies/ lametro/routes/704/stops/");
//		routes.put("Route_35", "http://api.metro.net/agencies/lametro/routes/35/sequence/");
//		routes.put("Route_111", "http://api.metro.net/agencies/lametro/routes/111/sequence/");
//		routes.put("Route_40", "http://api.metro.net/agencies/lametro/routes/40/sequence/");
//		routes.put("Route_10", "http://api.metro.net/agencies/lametro/routes/10/sequence/");
	}

	// Get URI
	public HashMap<String, String> getRoutes() {
		return routes;
	}

	// Set URI
	public void setRoutes(String route, String URI) {
		routes.put(route, URI);
	}
}
 