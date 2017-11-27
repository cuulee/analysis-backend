package com.conveyal.taui;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.conveyal.r5.analyst.broker.BrokerMain;
import com.conveyal.r5.analyst.cluster.AnalysisTask;
import com.conveyal.r5.analyst.cluster.AnalystWorker;
import com.conveyal.r5.analyst.cluster.GridResultAssembler;
import com.conveyal.r5.analyst.cluster.GridResultQueueConsumer;
import com.conveyal.r5.analyst.cluster.RegionalTask;
import com.conveyal.r5.analyst.cluster.TravelTimeSurfaceTask;
import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.r5.transit.TransportNetworkCache;
import com.conveyal.taui.persistence.GTFSPersistence;
import com.conveyal.taui.persistence.OSMPersistence;
import com.conveyal.taui.persistence.TiledAccessGrid;
import com.conveyal.taui.util.HttpUtil;
import com.google.common.io.ByteStreams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class LocalBroker {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBroker.class);

    private static final String ANALYSIS_WORKER_NAME = "ANALYSIS-BACKEND-WORKER";
    private static final String BROKER_NAME = "ANALYSIS-BACKEND-BROKER";
    private static final String QUEUE_CONSUMER_NAME = "ANALYSIS-BACKEND-QUEUE-CONSUMER";

    private static final String AWS_REGION = AnalysisServerConfig.region;
    private static final String BROKER_ADDRESS = "localhost";
    private static final int BROKER_PORT = 9001;
    private static final String GRAPHS_BUCKET = AnalysisServerConfig.bundleBucket;
    private static final String LOCAL_CACHE_DIR = AnalysisServerConfig.localCache;
    private static final String LOCAL_GRAPHS_CACHE_DIR = LOCAL_CACHE_DIR;
    private static final boolean OFFLINE = AnalysisServerConfig.offline;
    private static final String POINTSETS_BUCKET = AnalysisServerConfig.gridBucket; // TODO pointsets is another name???
    private static final String RESULTS_BUCKET = AnalysisServerConfig.resultsBucket;
    private static final String RESULTS_QUEUE = AnalysisServerConfig.resultsQueue;

    private static final AnalystWorker analysisWorker;
    private static final BrokerMain broker;

    private static final Thread analysisWorkerThread;
    private static final Thread brokerThread;

    private static GridResultQueueConsumer consumer;
    private static Thread consumerThread;
    public static String resultsQueueUrl;

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

        broker = new BrokerMain(brokerConfig);
        brokerThread = new Thread(broker, BROKER_NAME);

        // Create a single local woker to always be running
        Properties workerConfig = new Properties();

        workerConfig.setProperty("auto-shutdown", "false"); // cause that would be annoying
        workerConfig.setProperty("work-offline", "" + OFFLINE);
        workerConfig.setProperty("broker-address", BROKER_ADDRESS);
        workerConfig.setProperty("broker-port", "" + BROKER_PORT);
        workerConfig.setProperty("cache-dir", LOCAL_GRAPHS_CACHE_DIR);
        workerConfig.setProperty("pointsets-bucket", POINTSETS_BUCKET);

        TransportNetworkCache transportNetworkCache = new TransportNetworkCache(GTFSPersistence.cache, OSMPersistence.cache);
        analysisWorker = new AnalystWorker(workerConfig, transportNetworkCache);
        analysisWorkerThread = new Thread(analysisWorker, ANALYSIS_WORKER_NAME);

        // Start the threads
        analysisWorkerThread.start();
        brokerThread.start();

        // Initialize the grid consumer TODO enable a non-SQS mode
        if (!OFFLINE) {
            AmazonSQS sqs = new AmazonSQSClient();
            sqs.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(AWS_REGION)));
            resultsQueueUrl = sqs.getQueueUrl(RESULTS_QUEUE).getQueueUrl();
            consumer = new GridResultQueueConsumer(resultsQueueUrl, RESULTS_BUCKET);
            consumerThread = new Thread(consumer, QUEUE_CONSUMER_NAME);
            consumerThread.start();
        }
    }

    public static void deleteJob (String jobId) {
        broker.broker.deleteJob(jobId);
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

        broker.broker.enqueueTasks(tasks);
        consumer.registerJob(initialTask, new TilingGridResultAssembler(initialTask, RESULTS_BUCKET));
    }

    /**
     * Create a dummy response to pass to the broker and get it's OutputStream
     * @param task TravelTimeSurfaceTask
     * @return OutputStream the stream of data piped into the response object
     */
    public static byte[] getTravelTimeSurface(TravelTimeSurfaceTask task, Response response) throws IOException {
        HttpPost post = new HttpPost("http://" + BROKER_ADDRESS + ":" + BROKER_PORT + "/enqueue/single");
        post.setEntity(new StringEntity(JsonUtilities.objectMapper.writeValueAsString(task), ContentType.create("application/json", "utf-8")));

        try (CloseableHttpResponse brokerRes = HttpUtil.httpClient.execute(post)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = new BufferedInputStream(brokerRes.getEntity().getContent());
            ByteStreams.copy(is, baos);
            EntityUtils.consume(brokerRes.getEntity());
            brokerRes.close();

            response.status(brokerRes.getStatusLine().getStatusCode());
            response.type(brokerRes.getFirstHeader("Content-Type").getValue());

            return baos.toByteArray();
        }
    }

    /**
     * Get the grid result assembler for a job, used to create the status for an Analysis
     */
    public static GridResultAssembler getJob (String jobId) {
        return consumer.assemblers.get(jobId);
    }

    /**
     * A GridResultAssembler that tiles the results once they are complete
     * */
    public static class TilingGridResultAssembler extends GridResultAssembler {
        TilingGridResultAssembler(AnalysisTask request, String outputBucket) {
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
