package com.conveyal.taui.models;

import com.conveyal.r5.analyst.scenario.StopSpec;
import com.conveyal.taui.AnalysisServerException;
import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

class ModificationStop {
    private static double MIN_SPACING_PERCENTAGE = 0.25;

    StopSpec stop;
    double distanceFromStart;

    ModificationStop(Coordinate c, String id, double distanceFromStart) {
        this.stop = new StopSpec(c.x, c.y);
        this.stop.id = id;
        this.distanceFromStart = distanceFromStart;
    }

    static List<StopSpec> toSpec (List<ModificationStop> stops) {
        return stops.stream().map(s -> s.stop).collect(Collectors.toList());
    }

    static List<ModificationStop> getStopsFromSegments (List<Segment> segments) {
        Stack<ModificationStop> stops = new Stack<>();
        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;

        Segment firstSegment = segments.get(0);
        Coordinate firstStopCoord = firstSegment.geometry.getCoordinates()[0];
        ModificationStop firstStop = new ModificationStop(firstStopCoord, firstSegment.fromStopId, 0);
        stops.add(firstStop);

        double distanceToLastStop = 0;
        double distanceToLineSegmentStart = 0;
        for (Segment segment : segments) {
            Coordinate[] coords = segment.geometry.getCoordinates();
            int spacing = segment.spacing;
            for (int i = 1; i < coords.length; i++) {
                Coordinate c0 = coords[i - 1];
                Coordinate c1 = coords[i];
                double distanceThisLineSegment;
                try {
                    distanceThisLineSegment = JTS.orthodromicDistance(c0, c1, crs);
                } catch (TransformException e) {
                    throw AnalysisServerException.Unknown(e.getMessage());
                }

                if (spacing > 0) {
                    // Auto-created stops
                    while (distanceToLastStop + spacing < distanceToLineSegmentStart + distanceThisLineSegment) {
                        double frac = (distanceToLastStop + spacing - distanceToLineSegmentStart) / distanceThisLineSegment;
                        if (frac < 0) frac = 0;
                        Coordinate c = new Coordinate(c0.x + (c1.x - c0.x) * frac, c0.y + (c1.y - c0.y) * frac);

                        // We can't just add segment.spacing because of converting negative fractions to zero above.
                        // This can happen when the last segment did not have automatic stop creation, or had a larger
                        // spacing. TODO in the latter case, we probably want to continue to apply the spacing from the
                        // last line segment until we create a new stop?
                        distanceToLastStop = distanceToLineSegmentStart + frac * distanceThisLineSegment;

                        // Add the auto-created stop without an id
                        stops.add(new ModificationStop(c, null, distanceToLastStop));
                    }
                }

                distanceToLineSegmentStart += distanceThisLineSegment;
            }

            if (segment.toStopId != null) {
                // If the last auto-generated stop was too close, pop it
                ModificationStop lastStop = stops.peek();
                if (lastStop.stop.id == null && (distanceToLineSegmentStart - distanceToLastStop) / spacing < MIN_SPACING_PERCENTAGE) {
                    stops.pop();
                }

                Coordinate endCoord = coords[coords.length - 1];
                ModificationStop toStop = new ModificationStop(endCoord, segment.toStopId, distanceToLineSegmentStart);
                stops.add(toStop);
            }

            distanceToLastStop = distanceToLineSegmentStart;
        }

        return new ArrayList<>(stops);
    }

    static int[] getDwellTimes (List<ModificationStop> stops, Integer[] dwellTimes, int defaultDwellTime) {
        int[] stopDwellTimes = new int[stops.size()];

        int realStopIndex = 0;
        for (int i = 0; i < stops.size(); i++) {
            String id = stops.get(i).stop.id;
            if (id == null || dwellTimes == null) {
                stopDwellTimes[i] = defaultDwellTime;
            } else {
                Integer specificDwellTime = dwellTimes[realStopIndex];
                stopDwellTimes[i] = specificDwellTime != null ? specificDwellTime : defaultDwellTime;
                realStopIndex++;
            }
        }

        return stopDwellTimes;
    }

    static int[] getHopTimes (List<ModificationStop> stops, int[] segmentSpeeds) {
        int[] hopTimes = new int[stops.size() - 1];

        ModificationStop lastStop = stops.get(0);
        int realStopIndex = 0;
        for (int i = 1; i < stops.size(); i++) {
            ModificationStop stop = stops.get(i);
            double hopDistance = stop.distanceFromStart - lastStop.distanceFromStart;
            hopTimes[i - 1] = (int) (hopDistance / (segmentSpeeds[realStopIndex] * 1000) * 3000);

            if (stop.stop.id != null) {
                realStopIndex++;
            }

            lastStop = stop;
        }

        return hopTimes;
    }
}
