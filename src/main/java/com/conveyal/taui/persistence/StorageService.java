package com.conveyal.taui.persistence;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.taui.AnalysisServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class StorageService {
    private static final Logger LOG = LoggerFactory.getLogger(StorageService.class);

    private static final boolean OFFLINE = AnalysisServerConfig.offline;
    private static final String CACHE_DIR = AnalysisServerConfig.localCache;
    private static final String BUNDLE_DIR = AnalysisServerConfig.bundleBucket;
    private static final String GRID_DIR = AnalysisServerConfig.gridBucket;
    private static final String RESULTS_DIR = AnalysisServerConfig.resultsBucket;

    private static final AmazonS3 s3 = new AmazonS3Client();

    public static final Bucket Bundles = OFFLINE ? new FileBucket(BUNDLE_DIR) : new S3Bucket(BUNDLE_DIR);
    public static final Bucket Grids = OFFLINE ? new FileBucket(GRID_DIR) : new S3Bucket(GRID_DIR);
    public static final Bucket Results = OFFLINE ? new FileBucket(RESULTS_DIR) : new S3Bucket(RESULTS_DIR);

    public abstract static class Bucket {
        String bucketName;

        Bucket (String bucketName) {
            this.bucketName = bucketName;
        }

        public abstract boolean deleteObject (String key);
        public abstract boolean exists (String key);
        public abstract InputStream getObject (String key) throws IOException;
        public abstract OutputStream getOutputStream (String key, ObjectMetadata metadata) throws IOException;

        public OutputStream getOutputStream (String key) throws IOException {
            return getOutputStream(key, null);
        }
    }

    public static class FileBucket extends Bucket {
        FileBucket (String bucketName) {
            super(bucketName);

            // Make the local directory
            File file = new File(pathForKey(bucketName));
            file.mkdirs();
            file.delete();
        }

        String pathForKey (String key) {
            return String.format("%s/%s/%s", CACHE_DIR, bucketName, key);
        }

        public boolean deleteObject (String key) {
            File file = new File(pathForKey(key));
            return file.delete();
        }

        public boolean exists (String key) {
            File file = new File(pathForKey(key));
            return file.exists();
        }

        public InputStream getObject (String key) throws FileNotFoundException {
            return new FileInputStream(pathForKey(key));
        }

        public OutputStream getOutputStream (String key, ObjectMetadata metadata) throws FileNotFoundException {
            return new FileOutputStream(new File(pathForKey(key)));
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

        public InputStream getObject (String key) {
            return s3.getObject(bucketName, key).getObjectContent();
        }

        public OutputStream getOutputStream (String key, ObjectMetadata metadata) throws IOException {
            PipedInputStream pipedInputStream = new PipedInputStream();
            PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);

            s3.putObject(bucketName, key, pipedInputStream, metadata);

            return pipedOutputStream;
        }
    }
}
