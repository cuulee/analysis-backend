package com.conveyal.taui.persistence;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.api.ApiMain;
import com.conveyal.gtfs.api.models.FeedSource;
import com.conveyal.gtfs.api.util.FeedSourceCache;
import com.conveyal.taui.AnalysisServerConfig;

import java.io.File;
import java.util.function.Function;

public class GTFSPersistence {
    private static final boolean OFFLINE = AnalysisServerConfig.offline;
    private static final String LOCAL_CACHE_DIR = AnalysisServerConfig.localCache;

    public static final FeedSourceCache cache;

    static {
        File cacheDir = new File(LOCAL_CACHE_DIR);
        cacheDir.mkdirs();

        if (OFFLINE) {
            cache = ApiMain.initialize(null, LOCAL_CACHE_DIR);
        } else {
            cache = ApiMain.initialize(AnalysisServerConfig.bundleBucket, LOCAL_CACHE_DIR);
        }
    }

    public static FeedSource getFeedSource (String id) throws Exception {
        return ApiMain.getFeedSource(id);
    }

    public static FeedSource registerFeedSource (Function<GTFSFeed, String> idGenerator, File gtfsFile) throws Exception {
        return ApiMain.registerFeedSource(idGenerator, gtfsFile);
    }
}
