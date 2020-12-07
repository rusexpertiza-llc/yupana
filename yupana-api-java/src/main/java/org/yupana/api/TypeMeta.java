package org.yupana.api;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypeMeta {

    public TypeMeta(String sqlTypeName, int sqlType, int displaySize, Class<?> javaClass, int scale, int precision, boolean signed) {
        this.sqlTypeName = sqlTypeName;
        this.sqlType = sqlType;
        this.displaySize = displaySize;
        this.javaTypeName = javaClass != null ? javaClass.getCanonicalName() : "null";
        this.scale = scale;
        this.precision = precision;
        this.signed = signed;
    }

    private final String sqlTypeName;
    private final  int sqlType;
    private final int displaySize;
    private final String javaTypeName;
    private final int scale;
    private final int precision;
    private final boolean signed;

    public String getSqlTypeName() {
        return sqlTypeName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public String getJavaTypeName() {
        return javaTypeName;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

    public boolean isSigned() {
        return signed;
    }

    public static int MONEY_SCALE = 2;
    public static String ARRAY_PREFIX = "ARRAY[";
    public static String ARRAY_SUFFIX = "]";

    public static TypeMeta VARCHAR = new TypeMeta("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, String.class, 0, Integer.MAX_VALUE, false);
    public static TypeMeta BOOLEAN = new TypeMeta("BOOLEAN", Types.BOOLEAN, 5, Boolean.class, 0, 0, false);

    public static TypeMeta TINYINT = new TypeMeta("TINYINT", Types.TINYINT, 3, Byte.class, 0, 3, true);
    public static TypeMeta SMALLINT = new TypeMeta("SMALLINT", Types.SMALLINT, 5, Short.class, 0, 5, true);
    public static TypeMeta INTEGER = new TypeMeta("INTEGER", Types.INTEGER, 10, Integer.class, 0, 10, true);
    public static TypeMeta BIGINT = new TypeMeta("BIGINT", Types.BIGINT,20, Long.class, 0, 19, true);
    public static TypeMeta DECIMAL = decimalMeta(MONEY_SCALE);
    public static TypeMeta DOUBLE = new TypeMeta("DOUBLE", Types.DOUBLE, 10, Double.class, 17, 17, true);

    public static TypeMeta TIMESTAMP = new TypeMeta("TIMESTAMP", Types.TIMESTAMP, 23, Timestamp.class, 23, 6, false);
    public static TypeMeta PERIOD = new TypeMeta("PERIOD", Types.VARCHAR, 20, String.class, 20, 0, false);

    public static TypeMeta NULL = new TypeMeta("NULL", Types.NULL, 4, null, 0, 0, false);

    public static TypeMeta BLOB = new TypeMeta("BLOB", Types.BLOB, 10, Blob.class, 0, Integer.MAX_VALUE, false);

    public static TypeMeta decimalMeta(int scale) {
        return new TypeMeta("DECIMAL", Types.DECIMAL, 131089, BigDecimal.class, scale, 0, true);
    }

    public static TypeMeta arrayMeta(TypeMeta itemMeta) {
        return new TypeMeta(String.format("ARRAY[%s]", itemMeta.sqlTypeName), Types.ARRAY, 100, Object.class, 0, Integer.MAX_VALUE, false);
    }

    public static Optional<TypeMeta> byName(String name) {
        if (!name.startsWith(ARRAY_PREFIX) || !name.endsWith(ARRAY_SUFFIX)) {
            return Optional.ofNullable(metas.get(name));
        } else {
            String innerName = name.substring(TypeMeta.ARRAY_PREFIX.length(), name.length() - TypeMeta.ARRAY_SUFFIX.length());
            return byName(innerName).map(TypeMeta::arrayMeta);
        }
    }

    private static final Map<String, TypeMeta> metas = Stream.of(VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, NULL, BLOB, TIMESTAMP, PERIOD)
            .collect(Collectors.toMap(TypeMeta::getSqlTypeName, Function.identity()));

}
