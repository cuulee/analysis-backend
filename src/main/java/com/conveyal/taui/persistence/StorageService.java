package com.conveyal.taui.persistence;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.taui.AnalysisServerConfig;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class StorageService {
    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    private static final boolean OFFLINE = AnalysisServerConfig.offline;
    private static final String CACHE_DIR = AnalysisServerConfig.localCache;
    private static final String BUNDLE_DIR = AnalysisServerConfig.bundleBucket;
    private static final String GRID_DIR = AnalysisServerConfig.gridBucket;
    private static final String RESULTS_DIR = AnalysisServerConfig.resultsBucket;

    private static final AmazonS3 s3 = new AmazonS3Client();
    private static final ThreadPoolExecutor s3Upload = new ThreadPoolExecutor(4, 8, 90, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024));

    public static final Bucket Bundles = OFFLINE ? new FileBucket(BUNDLE_DIR) : new S3Bucket(BUNDLE_DIR);
    public static final Bucket Grids = OFFLINE ? new FileBucket(GRID_DIR) : new S3Bucket(GRID_DIR);
    public static final Bucket Results = OFFLINE ? new FileBucket(RESULTS_DIR) : new S3Bucket(RESULTS_DIR);

    static {
        File cacheDir = new File(CACHE_DIR);
        cacheDir.mkdirs();
    }

    public abstract static class Bucket {
        String bucketName;

        Bucket (String bucketName) {
            this.bucketName = bucketName;
        }

        public abstract boolean deleteObject (String key);
        public abstract boolean exists (String key);
        public abstract InputStream getInputStream(String key) throws IOException;
        public abstract OutputStream getOutputStream (String key, ObjectMetadata metadata) throws IOException;

        public OutputStream getOutputStream (String key) throws IOException {
            return getOutputStream(key, null);
        }

        public GZIPOutputStream getGZIPOutputStream (String key) throws IOException {
            return new GZIPOutputStream(getOutputStream(key));
        }

        public void retrieveAndRespond (Response response, String key, boolean isGzipped) throws IOException {
            if (isGzipped) {
                // Tell the client it's GZIPed binary data
                response.header("Content-Encoding", "gzip");
                response.type("application/octet-stream");
            }

            InputStream inputStream = getInputStream(key);
            OutputStream responseOutput = response.raw().getOutputStream();

            IOUtils.copy(inputStream, responseOutput);

            inputStream.close();
            responseOutput.close();
        }
    }

    public static class FileBucket extends Bucket {
        FileBucket (String bucketName) {
            super(bucketName);
        }

        public boolean deleteObject (String key) {
            File file = new File(CACHE_DIR, key);
            return file.delete();
        }

        public boolean exists (String key) {
            File file = new File(CACHE_DIR, key);
            return file.exists();
        }

        public InputStream getInputStream(String key) throws FileNotFoundException {
            return new FileInputStream(new File(CACHE_DIR, key));
        }

        public OutputStream getOutputStream (String key, ObjectMetadata metadata) throws IOException {
            File f = new File(CACHE_DIR, key);

            // If the key contains "/" the directory may need to be created
            f.getParentFile().mkdirs();

            FileOutputStream fileOutputStream = new FileOutputStream(f);
            if (metadata != null && "gzip".equals(metadata.getContentEncoding())) {
                return new GZIPOutputStream(fileOutputStream);
            } else {
                return fileOutputStream;
            }
        }
    }

    public static class S3Bucket extends Bucket {
        S3Bucket (String bucketName) {
            super(bucketName);
        }

        public boolean deleteObject (String key) {
            try {
                s3.deleteObject(bucketName, key);
            } catch (RuntimeException e) {
                LOG.error("Error deleting object from S3", e);
                return false;
            }
            return true;
        }

        public boolean exists (String key) {
            return s3.doesObjectExist(bucketName, key);
        }

        public InputStream getInputStream(String key) {
            return s3.getObject(bucketName, key).getObjectContent();
        }

        public OutputStream getOutputStream (String key, ObjectMetadata metadata) throws IOException {
            PipedInputStream pipedInputStream = new PipedInputStream();
            PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);

            s3Upload.execute(() -> s3.putObject(bucketName, key, pipedInputStream, metadata));

            if (metadata != null && "gzip".equals(metadata.getContentEncoding())) {
                return new GZIPOutputStream(pipedOutputStream);
            } else {
                return pipedOutputStream;
            }
        }
    }
}
