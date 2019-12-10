package com.cherry.calcite.csv.field;


import org.apache.calcite.linq4j.tree.Primitive;

import java.util.HashMap;
import java.util.Map;

public enum  CsvFieldType {
    STRING(String.class,"string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String,CsvFieldType>  MAP = new HashMap<>();

    static {
        for (CsvFieldType value : values()) {
            MAP.put(value.simpleName,value);
        }
    }

    CsvFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    CsvFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }
}
