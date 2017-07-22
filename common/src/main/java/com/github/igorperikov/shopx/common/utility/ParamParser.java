package com.github.igorperikov.shopx.common.utility;

import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.ext.web.RoutingContext;

import java.util.Optional;

public class ParamParser {
    public static Integer getIntValue(RoutingContext rc, String paramName, String defaultValue) {
        return Integer.valueOf(
                Optional.ofNullable(rc.request().getParam(paramName)).orElse(defaultValue)
        );
    }

    public static Integer getIntValue(RoutingContext rc, String paramName) {
        String paramValue = rc.request().getParam(paramName);
        if (paramValue == null) {
            rc.response()
                    .setStatusCode(400)
                    .end(new JsonObject().put("description", "specify '" + paramName + "' query param").encodePrettily());
            return 0;
        } else {
            return Integer.valueOf(paramValue);
        }
    }
}
