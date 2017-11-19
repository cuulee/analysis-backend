package com.conveyal.taui;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.conveyal.r5.analyst.broker.Broker;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.GridResultQueueConsumer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.transit.TransportNetworkCache;
import com.conveyal.taui.persistence.GTFSPersistence;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.TiledAccessGrid;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class LocalBroker {
    private static final String ANALYSIS_WORKER_NAME = "ANALYSIS-BACKEND-WORKER";
    private static final String BROKER_NAME = "ANALYSIS-BACKEND-BROKER";
    private static final String QUEUE_CONSUMER_NAME = "ANALYSIS-BACKEND-QUEUE-CONSUMER";

    private static final String AWS_REGION = AnalysisServerConfig.region;
    private static final String BROKER_ADDRESS = "localhost";
    private static final int BROKER_PORT = 9001;
    private static final String GRAPHS_BUCKET = AnalysisServerConfig.bundleBucket;
    private static final String LOCAL_CACHE_DIR = AnalysisServerConfig.localCache;
    private static final String LOCAL_GRAPHS_CACHE_DIR = LOCAL_CACHE_DIR + "/graphs";
    private static final boolean OFFLINE = AnalysisServerConfig.offline;
    private static final String POINTSETS_BUCKET = AnalysisServerConfig.gridBucket; // TODO pointsets is another name???
    private static final String RESULTS_BUCKET = AnalysisServerConfig.resultsBucket;

    private static final AnalystWorker analysisWorker;
    private static final Broker broker;
    private static final GridResultQueueConsumer consumer;

    private static final Thread analysisWorkerThread;
    private static final Thread brokerThread;
    private static final Thread consumerThread;

    public static final String resultsQueueUrl;

    static {
        // start the broker
        Properties brokerConfig = new Properties();

        brokerConfig.setProperty("auto-shutdown", "" + !OFFLINE);
        brokerConfig.setProperty("bind-address", BROKER_ADDRESS);
        brokerConfig.setProperty("cache-dir", LOCAL_GRAPHS_CACHE_DIR);
        brokerConfig.setProperty("graphs-bucket", GRAPHS_BUCKET);
        brokerConfig.setProperty("pointsets-bucket", POINTSETS_BUCKET);
        brokerConfig.setProperty("port", "" + BROKER_PORT);
        brokerConfig.setProperty("work-offline", "" + OFFLINE);

        // TODO set "max-workers" in AnalysisServerConfig?

        broker = new Broker(brokerConfig, BROKER_ADDRESS, BROKER_PORT);
        brokerThread = new Thread(broker, BROKER_NAME);

        // Create a single local woker to always be running
        Properties workerConfig = new Properties();

        workerConfig.setProperty("auto-shutdown", "false"); // cause that would be annoying
        workerConfig.setProperty("work-offline", "" + OFFLINE);
        workerConfig.setProperty("broker-address", BROKER_ADDRESS);
        workerConfig.setProperty("broker-port", "" + BROKER_PORT);
        workerConfig.setProperty("cache-dir", LOCAL_CACHE_DIR);
        workerConfig.setProperty("pointsets-bucket", POINTSETS_BUCKET);

        TransportNetworkCache transportNetworkCache = new TransportNetworkCache(GTFSPersistence.cache, OSMPersistence.cache);
        analysisWorker = new AnalystWorker(workerConfig, transportNetworkCache);
        analysisWorkerThread = new Thread(analysisWorker, ANALYSIS_WORKER_NAME);

        // Initialize the grid consumer TODO enable a non-SQS mode
        AmazonSQS sqs = new AmazonSQSClient();
        sqs.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(AWS_REGION)));
        resultsQueueUrl = sqs.getQueueUrl(AnalysisServerConfig.resultsQueue).getQueueUrl();
        consumer = new GridResultQueueConsumer(resultsQueueUrl, RESULTS_BUCKET);
        consumerThread = new Thread(consumer, QUEUE_CONSUMER_NAME);

        // Start the threads
        analysisWorkerThread.start();
        brokerThread.start();
        consumerThread.start();
    }

    public static void deleteJob (String jobId) {
        broker.deleteJob(jobId);
        consumer.deleteJob(jobId);
    }

    /**
     * Enqueue an AnalysisTask. Right now this creates height * width tasks and enqueues those. It shouldjust send the
     * broker a single task. TODO update when the broker can take a single regional task.
     * @param initialTask The initial task that is stored in a Regional Analysis
     */
    public static void enqueueRegionalTask(RegionalTask initialTask) {
        // Start the regional analysis
        List<AnalysisTask> tasks = new ArrayList<>();

        for (int x = 0; x < initialTask.width; x++) {
            for (int y = 0; y < initialTask.height; y++) {
                AnalysisTask nextTask = initialTask.clone();

                tasks.add(nextTask);
            }
        }

        broker.enqueueTasks(tasks);
        consumer.registerJob(initialTask, new TilingGridResultAssembler(initialTask, RESULTS_BUCKET));
    }

    /**
     * Create a dummy response to pass to the broker and get it's OutputStream
     * @param task TravelTimeSrufaceTask
     * @return OutputStream the stream of data piped into the response object
     */
    public static OutputStream getTravelTimeSurface(TravelTimeSurfaceTask task) {
        Request req = Request.create();
        Response res = req.getResponse();

        broker.enqueuePriorityTask(task, res);

        return res.getOutputStream();
    }

    /**
     * Get the job status
     */
    public static GridResultAssembler getJob (String jobId) {
        return consumer.assemblers.get(jobId);
    }

    /**
     * A GridResultAssembler that tiles the results once they are complete
     * */
    public static class TilingGridResultAssembler extends GridResultAssembler {
        public TilingGridResultAssembler(AnalysisTask request, String outputBucket) {
            super(request, outputBucket);
        }

        @Override
        protected synchronized void finish () {
            super.finish();
            // build the tiles (used to display sampling distributions in the client)
            // Note that the job will be marked as complete even before the tiles are built, but this is okay;
            // the tiles are not needed to display the regional analysis, only to display sampling distributions from it
            // the user can view the results immediately, and the sampling distribution loading will block until the tiles
            // are built thanks to the use of a Guava loadingCache below (which will only build the value for a particular key
            // once)
            try {
                TiledAccessGrid.get(request.jobId);
            } catch (ExecutionException e) {
                LOG.error("Error retrieving job from the TiledAccessGrid", e);
            }
        }
    }
}
