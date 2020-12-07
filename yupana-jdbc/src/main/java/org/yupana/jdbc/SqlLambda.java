package org.yupana.jdbc;

import java.sql.SQLException;
import java.util.function.Function;

@FunctionalInterface
interface SqlLambda<T, R> {
    R apply(T t) throws SQLException;

    default <Q> SqlLambda<T, Q> andThen(SqlLambda<R, Q> after) {
        return (T t) -> {
            R r = apply(t);
            return r != null ? after.apply(r) : null;
        };
    }
}
