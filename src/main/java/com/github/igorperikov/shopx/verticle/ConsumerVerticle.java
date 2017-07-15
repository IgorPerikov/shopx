package com.github.igorperikov.shopx.verticle;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Future;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.ext.healthchecks.HealthCheckHandler;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.client.HttpRequest;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.codec.BodyCodec;
import rx.Single;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class ConsumerVerticle extends AbstractVerticle {
    private WebClient webClient;

    @Override
    public void start() throws Exception {
        webClient = WebClient.create(vertx);
        Router router = Router.router(vertx);
        router.get("/check/:name1/:name2").handler(this::consume);
        HealthCheckHandler hch = HealthCheckHandler.create(vertx);
        hch.register("producerAvailability", event -> {
            webClient.get(8080, "localhost", "/test")
                    .rxSend()
                    .subscribeOn(RxHelper.scheduler(vertx))
                    .timeout(1, TimeUnit.SECONDS)
                    .map(HttpResponse::body)
                    .subscribe(
                            c -> event.complete(Status.OK()),
                            throwable -> {
                                throwable.printStackTrace();
                                event.fail(throwable);
                            }
                    );
        });
        hch.register("availabilityItself", Future::complete);
        router.get("/health").handler(hch);
        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(8081);
    }

    private void consume(RoutingContext rc) {
        HttpRequest<JsonObject> name1Response = webClient
                .get(8080, "localhost", "/" + rc.pathParam("name1"))
                .as(BodyCodec.jsonObject());
        HttpRequest<JsonObject> name2Response = webClient
                .get(8080, "localhost", "/" + rc.pathParam("name2"))
                .as(BodyCodec.jsonObject());

        Single<JsonObject> s1 = name1Response.rxSend()
                .map(HttpResponse::body)
                .onErrorReturn(throwable -> new JsonObject().put("name", rc.pathParam("name1")));
        Single<JsonObject> s2 = name2Response.rxSend()
                .map(HttpResponse::body)
                .onErrorReturn(throwable -> new JsonObject().put("name", rc.pathParam("name2")));

        Single.zip(s1, s2, (name1, name2) -> {
            return new JsonObject().put("name1", name1).put("name2", name2).put("requestTime", Instant.now());
        }).subscribe(
                entries -> rc.response().setStatusCode(200).end(entries.encodePrettily()),
                throwable -> {
                    throwable.printStackTrace();
                    rc.response().setStatusCode(500).end(throwable.getMessage());
                }
        );
    }
}
