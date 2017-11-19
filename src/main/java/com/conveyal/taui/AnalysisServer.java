package com.conveyal.taui;

import com.auth0.jwt.JWTVerifier;
import com.conveyal.taui.controllers.AggregationAreaController;
import com.conveyal.taui.controllers.BundleController;
import com.conveyal.taui.controllers.GraphQLController;
import com.conveyal.taui.controllers.ModificationController;
import com.conveyal.taui.controllers.OpportunityDatasetsController;
import com.conveyal.taui.controllers.ProjectController;
import com.conveyal.taui.controllers.RegionController;
import com.conveyal.taui.controllers.RegionalAnalysisController;
import com.conveyal.taui.controllers.SinglePointAnalysisController;
import com.google.common.io.CharStreams;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.imageio.ImageIO;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static spark.Spark.before;
import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.port;

/**
 * This is the main entry point for starting a Conveyal Analysis server.
 */
public class AnalysisServer {
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServer.class);

    private static final String API_PATH = "/api";
    private static final String DEFAULT_ACCESS_GROUP = "OFFLINE";
    private static final String DEFAULT_EMAIL = "analysis@conveyal.com";
    private static final int PORT = AnalysisServerConfig.port;

    public static void main (String... args) {
        LOG.info("Starting Conveyal Analysis server, the time is now {}", DateTime.now());

        // Set the port on which the HTTP server will listen for connections.
        LOG.info("Analysis server will listen for HTTP connections on port {}.", PORT);
        port(PORT);

        // initialize ImageIO
        // http://stackoverflow.com/questions/20789546
        ImageIO.scanForPlugins();

        // check if a user is authenticated
        before((req, res) -> {
            if (!req.pathInfo().startsWith(API_PATH)) return; // don't need to be authenticated to view main page

            // Default is JSON, will be overridden by the few controllers that do not return JSON
            res.type("application/json");

            // Log each API request
            LOG.info("{} {}", req.requestMethod(), req.pathInfo());

            if (!AnalysisServerConfig.offline) {
                handleAuthentication(req, res);
            } else {
                // hardwire group name if we're working offline
                req.attribute("accessGroup", DEFAULT_ACCESS_GROUP);
                req.attribute("email", DEFAULT_EMAIL);
            }
        });

        // Register all our HTTP request handlers with the Spark HTTP framework.
        RegionController.register();
        ModificationController.register();
        ProjectController.register();
        GraphQLController.register();
        BundleController.register();
        SinglePointAnalysisController.register();
        OpportunityDatasetsController.register();
        RegionalAnalysisController.register();
        AggregationAreaController.register();

        // Load index.html and register a handler with Spark to serve it up.
        InputStream indexStream = AnalysisServer.class.getClassLoader().getResourceAsStream("public/index.html");

        try {
            String index = CharStreams.toString(
                    new InputStreamReader(indexStream)).replace("${ASSET_LOCATION}", AnalysisServerConfig.assetLocation);
            indexStream.close();

            get("/*", (req, res) -> {
                res.type("text/html");
                return index;
            });
        } catch (IOException e) {
            LOG.error("Unable to load index.html");
            System.exit(1);
        }

        exception(AnalysisServerException.class, (e, request, response) -> {
            AnalysisServerException ase = ((AnalysisServerException) e);
            AnalysisServer.respondToException(response, ase, ase.type.name(), ase.message, ase.httpCode);
        });

        exception(Exception.class, (e, request, response) -> {
            AnalysisServer.respondToException(response, e, "UNKNOWN", e.getMessage(), 400);
        });

        exception(RuntimeException.class, (e, request, response) -> {
            AnalysisServer.respondToException(response, e, "UNKNOWN", e.getMessage(), 400);
        });

        LOG.info("Conveyal Analysis server is ready.");
    }

    private static void handleAuthentication (Request req, Response res) {
        String auth = req.headers("Authorization");

        // authorization required
        if (auth == null || auth.isEmpty()) {
            throw AnalysisServerException.Unauthorized("You must be logged in.");
        }

        // make sure it's properly formed
        String[] authComponents = auth.split(" ");

        if (authComponents.length != 2 || !"bearer".equals(authComponents[0].toLowerCase())) {
            throw AnalysisServerException.Unknown("Authorization header is malformed: " + auth);
        }

        // validate the JWT
        JWTVerifier verifier = new JWTVerifier(AnalysisServerConfig.auth0Secret, AnalysisServerConfig.auth0ClientId);

        Map<String, Object> jwt = null;
        try {
            jwt = verifier.verify(authComponents[1]);
        } catch (Exception e) {
            throw AnalysisServerException.Forbidden("Login failed to verify with our authorization provider. " + e.getMessage());
        }

        if (!jwt.containsKey("analyst")) {
            throw AnalysisServerException.Forbidden("Access denied. User does not have access to Analysis.");
        }

        String group = null;
        try {
            group = (String) ((Map<String, Object>) jwt.get("analyst")).get("group");
        } catch (Exception e) {
            throw AnalysisServerException.Forbidden("Access denied. User is not associated with any group. " + e.getMessage());
        }

        if (group == null) {
            throw AnalysisServerException.Forbidden("Access denied. User is not associated with any group.");
        }

        // attributes to be used on models
        req.attribute("accessGroup", group);
        req.attribute("email", jwt.get("email"));
    }

    private static void respondToException(Response response, Exception e, String type, String message, int code) {
        String stack = ExceptionUtils.getStackTrace(e);

        LOG.error("Server exception thrown, type: {}, message: {}", type, message);
        LOG.error(stack);

        JSONObject body = new JSONObject();
        body.put("type", type);
        body.put("message", message);
        body.put("stackTrace", stack);

        response.status(code);
        response.type("application/json");
        response.body(body.toJSONString());
    }
}
