package com.github.igorperikov.shopx.product;

import com.github.igorperikov.shopx.common.entities.Product;
import com.github.igorperikov.shopx.common.utility.ParamParser;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;

public class ProductVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ProductVerticle.class);
    private static final Integer PORT = 8080;
    private static final String SQL_GET_PAGE_OF_PRODUCTS = "SELECT * FROM products LIMIT ?,?";
    private static final String SQL_INSERT_NEW_PRODUCT = "INSERT INTO products (name) VALUES (?)";
    private static final String SQL_CHECK_CONNECTION = "SELECT * FROM products LIMIT 1";

    private JDBCClient dbClient;
    private ConfigRetriever configRetriever;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Future<Void> setup = initConfigRetriever()
                .compose(event -> obtainDatabaseConnection())
                .compose(event -> startWebServer());
        setup.setHandler(ar -> {
            if (ar.failed()) {
                startFuture.fail(ar.cause());
            } else {
                log.info("Server listening on port " + PORT);
                startFuture.complete();
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        dbClient.rxClose().subscribe(aVoid -> stopFuture.complete());
    }

    private Future<Void> initConfigRetriever() {
        return Future.future(event -> {
            ConfigStoreOptions jsonStore = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("json")
                    .setConfig(new JsonObject().put("path", "config.json"));
            ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(jsonStore);
            configRetriever = ConfigRetriever.create(vertx, options);
            configRetriever.getConfig(event1 -> {
                if (event1.succeeded()) {
                    event.complete();
                } else {
                    event.fail(event1.cause());
                }
            });
        });
    }

    private Future<Void> startWebServer() {
        Router router = Router.router(vertx);
        router.get("/products").handler(this::returnProducts);
        router.post("/products").handler(this::createProduct);

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

    private Future<Void> obtainDatabaseConnection() {
        return Future.future(event -> {
            configRetriever.getConfig(configEvent -> {
                if (configEvent.succeeded()) {
                    JsonObject jdbc = configEvent.result().getJsonObject("jdbc");
                    dbClient = JDBCClient.createShared(vertx, new JsonObject()
                            .put("provider_class", jdbc.getString("provider_class"))
                            .put("jdbcUrl", jdbc.getString("jdbcUrl"))
                            .put("username", jdbc.getString("username"))
                            .put("password", jdbc.getString("password"))
                            .put("connectionTimeout", jdbc.getInteger("connectionTimeout"))
                            .put("maximumPoolSize", jdbc.getInteger("maximumPoolSize")));
                    dbClient.rxGetConnection()
                            .flatMap(conn -> conn.rxQuery(SQL_CHECK_CONNECTION).doAfterTerminate(conn::close))
                            .subscribe(resultSet -> event.complete(), event::fail);
                } else {
                    event.fail(configEvent.cause());
                }
            });
        });
    }

    private void returnProducts(RoutingContext rc) {
        dbClient.rxGetConnection()
                .flatMap(sqlConnection -> {
                    Integer page = ParamParser.getIntValue(rc, "page", "1");
                    Integer rowCount = ParamParser.getIntValue(rc, "per_page", "10");
                    Integer rowOffset = rowCount * (page - 1);
                    return sqlConnection.rxQueryWithParams(
                            SQL_GET_PAGE_OF_PRODUCTS,
                            new JsonArray().add(rowOffset).add(rowCount)
                    ).doAfterTerminate(sqlConnection::close);
                })
                .subscribe(
                        resultSet -> {
                            rc.response()
                                    .putHeader("Content-type", "application/json")
                                    .end(new JsonObject().put("products", new JsonArray(resultSet.getRows())).encodePrettily());
                        },
                        rc::fail
                );
    }

    private void createProduct(RoutingContext rc) {
        // TODO: should it be executed under RxHelper.scheduler(vertx)?
        rc.request().toObservable().lift(RxHelper.unmarshaller(Product.class)).subscribe(product -> {
            dbClient.rxGetConnection()
                    .flatMap(conn -> {
                        return conn.rxUpdateWithParams(SQL_INSERT_NEW_PRODUCT, new JsonArray().add(product.name))
                                .doAfterTerminate(conn::close);
                    })
                    .subscribe(r -> rc.response().setStatusCode(201).end());
        });
    }
}
