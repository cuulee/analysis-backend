package com.conveyal.taui.controllers;

import com.conveyal.taui.util.JsonUtil;
import spark.Route;
import spark.Spark;

public class Controller {
    public static void getJson(String path, Route route) {
        Spark.get(path, route, JsonUtil.objectMapper::writeValueAsString);
    }
}
