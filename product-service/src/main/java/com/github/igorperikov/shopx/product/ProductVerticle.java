package com.github.igorperikov.shopx.product;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import rx.Single;

public class ProductVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ProductVerticle.class);
    private static final Integer PORT = 8080;

    private JDBCClient dbClient;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Future<Void> setup = obtainDatabaseConnection().compose(event -> startWebServer());
        setup.setHandler(ar -> {
            if (ar.failed()) {
                startFuture.fail(ar.cause());
            } else {
                log.info("Server listening on port " + PORT);
                startFuture.complete();
            }
        });
    }

    private Future<Void> startWebServer() {
        Router router = Router.router(vertx);
        router.get("/ping").handler(this::ping);

        return Future.future(event -> {
            vertx.createHttpServer()
                    .requestHandler(router::accept)
                    .rxListen(PORT)
                    .subscribe(
                            httpServer -> event.complete(),
                            event::fail
                    );
        });
    }

    private void ping(RoutingContext rc) {
        rc.response()
                .putHeader("Content-type", "application/json")
                .end(new JsonObject().put("status", "ok").encodePrettily());
    }

    private Future<Void> obtainDatabaseConnection() {
        return Future.future(event -> {
            dbClient = JDBCClient.createNonShared(vertx, new JsonObject()
                    .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider")
                    .put("jdbcUrl", "jdbc:mysql://localhost:3306/shopx?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false")
                    .put("username", "root")
                    .put("password", "root")
                    .put("connectionTimeout", 1000)
                    .put("maximumPoolSize", 64));

            dbClient.rxGetConnection()
                    .flatMap(
                            sqlConnection -> {
                                Single<ResultSet> querySingle = sqlConnection.rxQuery("select * from products limit 1");
                                return querySingle.doAfterTerminate(sqlConnection::close);
                            }
                    )
                    .subscribe(
                            resultSet -> event.complete(),
                            event::fail
                    );
        });
    }
}
