package com.conveyal.taui.persistence;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.r5.analyst.Grid;
import com.conveyal.r5.analyst.SelectingGridReducer;
import com.conveyal.r5.analyst.cluster.AccessGridWriter;
import com.conveyal.r5.util.S3Util;
import com.conveyal.taui.AnalysisServerConfig;
import com.conveyal.taui.util.JsonUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A TiledAccessGrid is an access grid split up into many smaller files to allow random access. This is used for
 * retrieving the sampling distribution at a particular point in a regional analysis for display in the client.
 */
public class TiledAccessGrid {
    private static final String HEADER_STRING = "ACCESSGR";
    private static final Logger LOG = LoggerFactory.getLogger(TiledAccessGrid.class);
    private static final String RESULTS_BUCKET = AnalysisServerConfig.resultsBucket;

    /** Size of one tile */
    private static final int TILE_SIZE = 64;

    /**
     * Version of TiledAccessGrid format.
     * This is used for simple S3 cachebusting by putting the version in the filename. If you change the format (including the
     * chunk size, below), increment this number to cause the indices to be rebuilt.
     */
    private static final int VERSION = 0;

    /** Key of acccess grid we're indexing */
    private final String key;

    /** The header of the access grid */
    private Header header;

    /** LoadingCache used so that repeated requests while index is building won't build index multiple times */
    private static LoadingCache<String, TiledAccessGrid> cache = CacheBuilder.newBuilder()
            .maximumSize(5)
            .build(new CacheLoader<String, TiledAccessGrid>() {
                @Override
                public TiledAccessGrid load(String key) {
                    return new TiledAccessGrid(key);
                }
            });


    private static File cacheDir = new File(AnalysisServerConfig.localCache, "acccess-grids");

    /** Cache access grid tiles on local disk so we don't always have to pull from S3 */
    private static LoadingCache<String, File> tileCache = CacheBuilder.newBuilder()
            .maximumSize(50)
            // delete files when they drop out of the cache.
            // It is possible this would delete a file someone is using, but unlikely, because the files are only open for
            // a matter of milliseconds after being pulled from this cache.
            .removalListener((RemovalListener<String, File>) removalNotification -> removalNotification.getValue().delete())
            .build(new CacheLoader<String, File>() {
                @Override
                public File load(String key) throws Exception {
                    File bucketDir = new File(cacheDir, RESULTS_BUCKET);
                    bucketDir.mkdirs();
                    File cacheFile = new File(bucketDir, key);
                    // gunzip here so that we can do random access within file.
                    InputStream is =
                            new GZIPInputStream(StorageService.Results.getInputStream(key));
                    OutputStream os = new BufferedOutputStream(new FileOutputStream(cacheFile));
                    ByteStreams.copy(is, os);
                    is.close();
                    os.close();

                    cacheFile.deleteOnExit(); // don't let these pile up when the app exits
                    return cacheFile;
                }
            });

    /**
     * This is private; use the static get method below. We do this so we can hand off to Guava issues of making sure
     * this isn't called multiple times for the same grid.
     */
    private TiledAccessGrid(String key) {
        this.key = key;

        readOrBuild();
    }

    /** Get the S3 key of the tile containing a particular coordinate in the original grid */
    private String getTileKey (int x, int y) {
        // int divide will floor
        return String.format("%s_index_v%d_%d_%d", key, VERSION, x / TILE_SIZE, y / TILE_SIZE);
    }

    private String getHeaderKey () {
        return String.format("%s_index_v%d_header.json", key, VERSION);
    }

    /** Check if the tiles are already built, read the header if they are, and build them otherwise */
    private void readOrBuild () {
        if (StorageService.Results.exists(getHeaderKey())) {
            read();
        } else {
            build();
        }
    }

    /** Build the tiles for an existing access grid, storing the files in S3. No need to synchronize, only called from constructor */
    private void build () {
        LOG.info("Indexed access grid for {} was not found, building it", this.key);
        try {
            Map<String, AccessGridWriter> files = new HashMap<>();
            File tempDir = Files.createTempDir();

            header = new Header();

            new SelectingGridReducer (0) {
                @Override
                protected double computeValueForOrigin(int x, int y, int[] valuesThisOrigin, int zoom, int west, int north, int width, int height) {
                    String tile = getTileKey(x, y);
                    AccessGridWriter writer = files.computeIfAbsent(tile, t -> {
                        try {
                            return new AccessGridWriter(
                                    new File(tempDir, tile),
                                    zoom,
                                    west + x / TILE_SIZE,
                                    north + y / TILE_SIZE,
                                    TILE_SIZE,
                                    TILE_SIZE,
                                    valuesThisOrigin.length);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (header.zoom == -1) {
                        header.zoom = zoom;
                        header.north = north;
                        header.west = west;
                        header.width = width;
                        header.height = height;
                        header.nValuesPerPixel = valuesThisOrigin.length;
                    }

                    try {
                        writer.writePixel(x % TILE_SIZE, y % TILE_SIZE, valuesThisOrigin);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    return 0;
                }
            }.compute(RESULTS_BUCKET, key);

            LOG.info("Converted access grid {} to {} tiles", key, files.size());

            // write all tiles to S3
            files.forEach((key, writer) -> {
                try {
                    writer.close();
                    File gzipFile = new File(tempDir, writer.file.getName() + ".gz");
                    InputStream is = new BufferedInputStream(new FileInputStream(writer.file));
                    OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(gzipFile)));
                    ByteStreams.copy(is, os);
                    is.close();
                    os.close();
                    S3Util.s3.putObject(RESULTS_BUCKET, key, gzipFile);
                    writer.file.delete();
                    gzipFile.delete();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            // Don't forget the header!
            // it's fine to just store this in memory, it's small
            ByteArrayOutputStream headerOutputStream = new ByteArrayOutputStream();
            JsonUtil.objectMapper.writeValue(headerOutputStream, header);
            headerOutputStream.close();
            InputStream headerInputStream = new ByteArrayInputStream(headerOutputStream.toByteArray());
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/json");
            S3Util.s3.putObject(RESULTS_BUCKET, getHeaderKey(), headerInputStream, metadata);
            headerInputStream.close();

            LOG.info("Done saving tiled access grid to S3");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Read an already-tiled access grid */
    private void read () {
        try {
            InputStream is = StorageService.Results.getInputStream(getHeaderKey());
            this.header = JsonUtil.objectMapper.readValue(is, Header.class);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get the values at the given coordinates in the original access grid (relative to the grid west and north edges) */
    private int[] getGridCoordinates (int x, int y) throws ExecutionException, IOException {
        // return all zeros if we're outside the grid
        if (x < 0 || y < 0 || x >= header.width || y >= header.height) return new int[header.nValuesPerPixel];

        String tileKey = getTileKey(x, y);

        // transform x and y to be tile relative
        int tileRelativeX = x % TILE_SIZE;
        int tileRelativeY = y % TILE_SIZE;

        File cacheFile = tileCache.get(tileKey);
        RandomAccessFile tile = new RandomAccessFile(cacheFile, "r");

        byte[] header = new byte[8];
        for (int i = 0; i < 8; i++) header[i] = tile.readByte();
        String headerStr = new String(header);

        if (!headerStr.equals(HEADER_STRING)) throw new IllegalStateException("Tile is not in Access Grid format!");
        if (tile.readInt() != 0) throw new IllegalStateException("Invalid access grid tile version!");

        tile.seek(24); // seek to width
        int width = readIntLittleEndian(tile);
        int height = readIntLittleEndian(tile);
        if (width != TILE_SIZE || height != TILE_SIZE) throw new IllegalStateException("Invalid access grid tile size!");

        long nValuesPerPixel = readIntLittleEndian(tile);
        // 4 bytes per value
        long offset = AccessGridWriter.HEADER_SIZE + ((long) tileRelativeY * (long) width + (long) tileRelativeX) * (long) nValuesPerPixel * 4L;
        tile.seek(offset);

        int[] values = new int[(int) nValuesPerPixel];

        // read and de-delta-code
        for (int i = 0, val = 0; i < nValuesPerPixel; i++) {
            values[i] = (val += readIntLittleEndian(tile));
        }

        return values;
    }

    /** Get the values at a particular latitude and longitude in this TiledAccessGrid */
    public int[] getLatLon (double lat, double lon) throws ExecutionException, IOException {
        int x = Grid.lonToPixel(lon, header.zoom) - header.west;
        int y = Grid.latToPixel(lat, header.zoom) - header.north;
        return getGridCoordinates(x, y);
    }

    private static String idToAccessFileName (String id) {
        return String.format("%s.access", id);
    }

    public static TiledAccessGrid get (String id) throws ExecutionException {
        return cache.get(idToAccessFileName(id));
    }

    /** this class will be JSON-serialized to represent header of original access grid */
    private static class Header {
        int zoom = -1;
        int west = -1;
        int north = -1;
        int width = -1;
        int height = -1;
        int nValuesPerPixel = -1;
    }

    /** The default read methods all use "network byte order" (big-endian), write a custom function using little-endian byte order */
    private static int readIntLittleEndian (RandomAccessFile file) throws IOException {
        byte b1 = (byte) file.read();
        byte b2 = (byte) file.read();
        byte b3 = (byte) file.read();
        byte b4 = (byte) file.read();
        return Ints.fromBytes(b4, b3, b2, b1);
    }
}

