package com.github.igorperikov.shopx.verticle;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.time.Instant;

public class ProducerVerticle extends AbstractVerticle {
    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        router.get("/").handler(this::hello);
        router.get("/:name").handler(this::hello);

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void hello(RoutingContext rc) {
        String name = rc.pathParam("name");
        String message;
        if (name != null) {
            message = "hello " + name;
        } else {
            message = "hello fellow";
        }
        JsonObject json = new JsonObject()
                .put("message", message)
                .put("date", Instant.now().toString());
        rc.response()
                .putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .end(json.encodePrettily());
    }
}
