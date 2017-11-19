package com.conveyal.taui.models;

import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.scenario.Modification;
import com.conveyal.r5.analyst.scenario.Scenario;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.taui.persistence.Persistence;
import com.mongodb.QueryBuilder;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class AnalysisRequest {
    private static int ZOOM = 9;

    public String regionId;
    public String projectId;
    public int variantIndex;
    public String bundleId;
    public String workerVersion;

    public String accessModes;
    public String directModes;
    public String egressModes;
    public float fromLat;
    public float fromLon;
    public int fromTime;
    public int toTime;
    public String transitModes;

    // Regional only
    public Bounds bounds;
    public Integer maxTripDurationMinutes;
    public String name;
    public String opportunityDatasetKey;
    public Integer travelTimePercentile;

    /**
     * Get all of the modifications for a project id that are in the Variant and map them to their corresponding r5 mod
     */
    private static List<Modification> modificationsForProject (String accessGroup, String projectId, int variantIndex) {
        return Persistence.modifications
                .findPermitted(QueryBuilder.start("projectId").is(projectId).get(), accessGroup)
                .stream()
                .filter(m -> m.variants[variantIndex]).map(com.conveyal.taui.models.Modification::toR5)
                .collect(Collectors.toList());
    }

    /**
     * Finds the modifications for the specified project and variant, maps them to their corresponding R5 modification
     * types, creates a checksum from those modifications, and adds them to the AnalysisTask along with the rest of the
     * request.
     */
    public AnalysisTask populateTask (AnalysisTask task, String accessGroup) {
        List<Modification> modifications = modificationsForProject(accessGroup, projectId, variantIndex);

        // No idea how long this operation takes or if it is actually necessary
        CRC32 crc = new CRC32();
        crc.update(modifications.stream().map(Modification::toString).collect(Collectors.joining("-")).getBytes());

        task.scenario = new Scenario();
        // TODO figure out why we use both
        task.scenario.id = task.scenarioId = String.format("%s-%s-%s", projectId, variantIndex, crc.getValue());
        task.scenario.modifications = modifications;

        task.graphId = bundleId;

        if (travelTimePercentile == null) {
            task.percentiles = new double[]{5, 25, 50, 75, 95};
        } else {
            task.percentiles = new double[]{travelTimePercentile};
        }

        task.workerVersion = workerVersion;

        Bounds b = bounds;
        if (b == null) {
            Region region = Persistence.regions.findByIdIfPermitted(regionId, accessGroup);
            b = region.bounds;
        }

        int east = Grid.lonToPixel(b.east, ZOOM);
        int north = Grid.latToPixel(b.north, ZOOM);
        int south = Grid.latToPixel(b.south, ZOOM);
        int west = Grid.lonToPixel(b.west, ZOOM);

        task.height = south - north;
        task.north = north;
        task.west = west;
        task.width = east - west;
        task.zoom = ZOOM;

        task.fromLat = fromLat;
        task.fromLon = fromLon;
        task.fromTime = fromTime;
        task.toTime = toTime;

        if (maxTripDurationMinutes != null) {
            task.maxTripDurationMinutes = maxTripDurationMinutes;
        }

        task.accessModes = EnumSet.copyOf(Arrays.stream(accessModes.split(","))
                .map(LegMode::valueOf).collect(Collectors.toList()));
        task.directModes = EnumSet.copyOf(Arrays.stream(directModes.split(","))
                .map(LegMode::valueOf).collect(Collectors.toList()));
        task.egressModes = EnumSet.copyOf(Arrays.stream(egressModes.split(","))
                .map(LegMode::valueOf).collect(Collectors.toList()));
        task.transitModes = EnumSet.copyOf(Arrays.stream(transitModes.split(","))
                .map(TransitModes::valueOf).collect(Collectors.toList()));

        return task;
    }
}
