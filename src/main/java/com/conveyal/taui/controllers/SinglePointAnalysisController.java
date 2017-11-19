package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.LocalBroker;
import com.conveyal.taui.models.AnalysisRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.OutputStream;

import static spark.Spark.post;

/**
 * Handles talking to the broker.
 */
public class SinglePointAnalysisController {
    private static Logger LOG = LoggerFactory.getLogger(SinglePointAnalysisController.class);

    /**
     * Parses the incoming Single Point Analysis request from the UI and sends it to the Broker
     */
    private static OutputStream analysis (Request req, Response res) throws IOException {
        AnalysisRequest analysisRequest = JsonUtilities.objectMapper.readValue(req.body(), AnalysisRequest.class);
        TravelTimeSurfaceTask task = (TravelTimeSurfaceTask) analysisRequest.populateTask(new TravelTimeSurfaceTask(), req.attribute("accessGroup"));
        LOG.info("Single point request by {} made", req.attribute("email").toString());

        return LocalBroker.getTravelTimeSurface(task);
    }

    public static void register () {
        post("/api/analysis", SinglePointAnalysisController::analysis);
    }
}
