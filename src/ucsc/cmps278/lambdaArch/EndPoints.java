package ucsc.cmps278.lambdaArch;

import java.util.HashMap;

/**
 *	The logic to read this end points configuration from a file 
 *	and populating this map can done later. Keep it in-memory for now.
 */
public class EndPoints {
	HashMap<String, String> routes =  new HashMap<>();

	// Ctor
	public EndPoints() {
		// Later on read external configuration files here
		routes.put("Route_704", "http://api.metro.net/agencies/lametro/routes/704/vehicles/");
	}

	// Get URI
	public String getRoutes(String route) {
		return routes.get(route);
	}

	// Set URI
	public void setRoutes(String route, String URI) {
		routes.put(route, URI);
	}
}
 