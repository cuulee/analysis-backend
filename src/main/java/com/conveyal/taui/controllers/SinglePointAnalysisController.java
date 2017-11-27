package com.conveyal.taui.controllers;

import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.LocalBroker;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;

import static spark.Spark.post;

/**
 * Handles talking to the broker.
 */
public class SinglePointAnalysisController {
    private static Logger LOG = LoggerFactory.getLogger(SinglePointAnalysisController.class);

    /**
     * Parses the incoming Single Point Analysis request from the UI and sends it to the Broker
     */
    private static byte[] analysis (Request req, Response res) throws IOException {
        AnalysisRequest analysisRequest = JsonUtilities.objectMapper.readValue(req.body(), AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, req.attribute("accessGroup"));
        TravelTimeSurfaceTask task = (TravelTimeSurfaceTask) analysisRequest.populateTask(new TravelTimeSurfaceTask(), project);
        LOG.info("Single point request by {} made", req.attribute("email").toString());

        return LocalBroker.getTravelTimeSurface(task, res);
    }

    public static void register () {
        post("/api/analysis", SinglePointAnalysisController::analysis);
    }
}
