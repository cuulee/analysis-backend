package com.conveyal.taui.models;

import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;

/**
 * Represents a query.
 */
public class RegionalAnalysis extends Model implements Cloneable {
    public String regionId;
    public String projectId;
    public int variantIndex;
    public String bundleId;

    public RegionalTask request;

    /** Is this Analysis complete? */
    public boolean complete;

    /** Has this analysis been (soft) deleted? */
    public boolean deleted;

    /** Is this Owen style accessibility? */
    public boolean isOwen = false;

    public static final class RegionalAnalysisStatus implements Serializable {
        public int total = -1;
        public int complete = -1;

        RegionalAnalysisStatus () {}

        public RegionalAnalysisStatus (GridResultAssembler assembler) {
            total = assembler.nTotal;
            complete = assembler.nComplete;
        }
    }

    // TODO do statuses differently
    @JsonView(JsonViews.Api.class)
    public RegionalAnalysisStatus getStatus () {
        return new RegionalAnalysisStatus();
        // GridResultAssembler assembler = LocalBroker.getJob(this._id);
        // return assembler != null ? new RegionalAnalysisStatus(assembler) : null;
    }

    /**
     * Using JsonViews doesn't work in the database currently due to https://github.com/mongojack/mongojack/issues/145,
     * so the status is saved in the db if anything about the regional analysis is changed.
     */
    public void setStatus (RegionalAnalysisStatus status) {
        // status is not intended to be persisted, ignore it.
    }

    public RegionalAnalysis clone () {
        try {
            return (RegionalAnalysis) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
