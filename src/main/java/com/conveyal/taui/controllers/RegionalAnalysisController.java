package com.conveyal.taui.controllers;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.BootstrapPercentileMethodHypothesisTestGridReducer;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.SelectingGridReducer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.LocalBroker;
import com.conveyal.taui.models.AnalysisRequest;
import com.conveyal.taui.models.Project;
import com.conveyal.taui.models.RegionalAnalysis;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.persistence.StorageService;
import com.conveyal.taui.persistence.TiledAccessGrid;
import com.conveyal.taui.util.JsonUtil;
import com.mongodb.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Created by matthewc on 10/21/16.
 */
public class RegionalAnalysisController {
    private static final Logger LOG = LoggerFactory.getLogger(RegionalAnalysisController.class);

    private static Collection<RegionalAnalysis> getRegionalAnalysis (Request req, Response res) {
        return Persistence.regionalAnalyses.findPermitted(
                QueryBuilder.start().and(
                        QueryBuilder.start("regionId").is(req.params("regionId")).get(),
                        QueryBuilder.start("deleted").is(false).get()
                ).get(),
                req.attribute("accessGroup")
        );
    }

    private static RegionalAnalysis deleteRegionalAnalysis (Request req, Response res) {
        String accessGroup = req.attribute("accessGroup");
        String email = req.attribute("email");

        RegionalAnalysis analysis = Persistence.regionalAnalyses.findByIdFromRequestIfPermitted(req);
        analysis.deleted = true;
        Persistence.regionalAnalyses.updateByUserIfPermitted(analysis, email, accessGroup);

        // clear it from the broker
        LocalBroker.deleteJob(analysis._id);

        return analysis;
    }

    private static void validateFormat(String format) {
        if (!"grid".equals(format) && !"png".equals(format) && !"tiff".equals(format)) {
            throw AnalysisServerException.BadRequest("Format \"" + format + "\" is invalid. Request format must be \"grid\", \"png\", or \"tiff\".");
        }
    }

    /** Get a particular percentile of a query as a grid file */
    private static Object getPercentile (Request req, Response res) throws IOException {
        String regionalAnalysisId = req.params("_id");

        // while we can do non-integer percentiles, don't allow that here to prevent cache misses
        String format = req.params("format").toLowerCase();
        validateFormat(format);

        String percentileGridKey = String.format("%s_given_percentile_travel_time.%s", regionalAnalysisId, format);
        String accessGridKey = String.format("%s.access", regionalAnalysisId);

        ObjectMetadata om = new ObjectMetadata();
        if ("grid".equals(format)) {
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");
        } else if ("png".equals(format)) {
            om.setContentType("image/png");
        } else if ("tiff".equals(format)) {
            om.setContentType("image/tiff");
        }

        if (!StorageService.Results.exists(percentileGridKey)) {
            long computeStart = System.currentTimeMillis();
            // make the grid
            Grid grid = new SelectingGridReducer(0).compute(StorageService.Results.getInputStream(accessGridKey));
            LOG.info("Building grid took {}s", (System.currentTimeMillis() - computeStart) / 1000d);

            OutputStream outputStream = StorageService.Results.getOutputStream(percentileGridKey, om);

            if ("grid".equals(format)) {
                grid.write(outputStream);
            } else if ("png".equals(format)) {
                grid.writePng(outputStream);
            } else if ("tiff".equals(format)) {
                grid.writeGeotiff(outputStream);
            }
        }

        StorageService.Results.retrieveAndRespond(res, percentileGridKey, "gzip".equals(om.getContentEncoding()));

        return null;
    }

    /** Get a probability of improvement from a baseline to a project */
    private static Object getProbabilitySurface (Request req, Response res) throws IOException {
        String base = req.params("baseId");
        String project = req.params("projectId");
        String format = req.params("format").toLowerCase();
        validateFormat(format);
        String probabilitySurfaceKey = String.format("%s_%s_probability.%s", base, project, format);

        ObjectMetadata om = new ObjectMetadata();
        if ("grid".equals(format)) {
            om.setContentType("application/octet-stream");
            om.setContentEncoding("gzip");
        } else if ("png".equals(format)) {
            om.setContentType("image/png");
        } else if ("tiff".equals(format)) {
            om.setContentType("image/tiff");
        }

        if (!StorageService.Results.exists(probabilitySurfaceKey)) {
            LOG.info("Probability surface for {} -> {} not found, building it", base, project);

            String baseKey = String.format("%s.access", base);
            String projectKey = String.format("%s.access", project);

            // if these are bootstrapped travel times with a particular travel time percentile, use the bootstrap
            // p-value/hypothesis test computer. Otherwise use the older setup.
            // TODO should all comparisons use the bootstrap computer? the only real difference is that it is two-tailed.
            BootstrapPercentileMethodHypothesisTestGridReducer computer = new BootstrapPercentileMethodHypothesisTestGridReducer();
            Grid grid = computer.computeImprovementProbability(StorageService.Results.getInputStream(baseKey), StorageService.Results.getInputStream(projectKey));

            OutputStream outputStream = StorageService.Results.getOutputStream(probabilitySurfaceKey, om);

            if ("grid".equals(format)) {
                grid.write(outputStream);
            } else if ("png".equals(format)) {
                grid.writePng(outputStream);
            } else if ("tiff".equals(format)) {
                grid.writeGeotiff(outputStream);
            }
        }

        StorageService.Results.retrieveAndRespond(res, probabilitySurfaceKey, "gzip".equals(om.getContentEncoding()));

        return null;
    }

    private static int[] getSamplingDistribution (Request req, Response res) {
        String regionalAnalysisId = req.params("_id");
        double lat = Double.parseDouble(req.params("lat"));
        double lon = Double.parseDouble(req.params("lon"));

        try {
            return TiledAccessGrid
                    .get(regionalAnalysisId)
                    .getLatLon(lat, lon);
        } catch (Exception e) {
            throw AnalysisServerException.Unknown(e);
        }
    }

    private static RegionalAnalysis createRegionalAnalysis (Request req, Response res) throws IOException {
        final String accessGroup = req.attribute("accessGroup");
        final String email = req.attribute("email");

        AnalysisRequest analysisRequest = JsonUtil.objectMapper.readValue(req.body(), AnalysisRequest.class);
        Project project = Persistence.projects.findByIdIfPermitted(analysisRequest.projectId, accessGroup);
        RegionalTask task = (RegionalTask) analysisRequest.populateTask(new RegionalTask(), project);

        task.grid = String.format("%s/%s.grid", project.regionId, analysisRequest.opportunityDatasetKey);
        task.outputQueue = LocalBroker.resultsQueueUrl;
        task.x = 0;
        task.y = 0;

        RegionalAnalysis regionalAnalysis = new RegionalAnalysis();

        regionalAnalysis.accessGroup = accessGroup;
        regionalAnalysis.createdBy = email;
        regionalAnalysis.regionId = project.regionId;
        regionalAnalysis.projectId = analysisRequest.projectId;
        regionalAnalysis.name = analysisRequest.name;
        regionalAnalysis.request = task;

        regionalAnalysis = Persistence.regionalAnalyses.create(regionalAnalysis);
        regionalAnalysis.request.jobId = regionalAnalysis._id;
        regionalAnalysis = Persistence.regionalAnalyses.put(regionalAnalysis);

        LocalBroker.enqueueRegionalTask(regionalAnalysis.request);

        return regionalAnalysis;
    }

    public static void register () {
        get("/api/region/:regionId/regional", RegionalAnalysisController::getRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        get("/api/regional/:_id/grid/:format", RegionalAnalysisController::getPercentile);
        get("/api/regional/:_id/samplingDistribution/:lat/:lon", RegionalAnalysisController::getSamplingDistribution);
        get("/api/regional/:baseId/:projectId/:format", RegionalAnalysisController::getProbabilitySurface);
        delete("/api/regional/:_id", RegionalAnalysisController::deleteRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
        post("/api/regional", RegionalAnalysisController::createRegionalAnalysis, JsonUtil.objectMapper::writeValueAsString);
    }

}
