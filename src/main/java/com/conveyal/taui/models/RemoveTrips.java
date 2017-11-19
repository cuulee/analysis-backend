package com.conveyal.taui.models;

/**
 * Remove trips from a graph.
 */
public class RemoveTrips extends Modification {
    public String getType () {
        return "remove-trips";
    }

    public String feed;

    public String[] routes;

    public String[] trips;

    public com.conveyal.r5.analyst.scenario.RemoveTrips toR5 () {
        com.conveyal.r5.analyst.scenario.RemoveTrips rt = new com.conveyal.r5.analyst.scenario.RemoveTrips();

        if (trips == null) {
            rt.routes = feedScopeIds(feed, routes);
        } else {
            rt.patterns = feedScopeIds(feed, trips);
        }

        return rt;
    }
}
