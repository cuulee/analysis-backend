package com.conveyal.taui.util;

import org.apache.commons.io.IOUtils;
import spark.Response;

import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SparkResponse {
    static void respondWithInputStream (Response response, InputStream input) throws IOException {
        ServletOutputStream output = response.raw().getOutputStream();

        IOUtils.copy(input, output);

        input.close();
        output.close();
    }
}
