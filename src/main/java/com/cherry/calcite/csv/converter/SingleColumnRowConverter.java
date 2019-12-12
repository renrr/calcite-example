package com.cherry.calcite.csv.converter;

import com.cherry.calcite.csv.field.CsvFieldType;

public class SingleColumnRowConverter extends RowConverter{

    private final CsvFieldType fieldType;
    private final int fieldIndex;

    public SingleColumnRowConverter(CsvFieldType fieldType, int fieldIndex) {
        this.fieldType = fieldType;
        this.fieldIndex = fieldIndex;
    }

    public Object convertRow(String[] strings) {
        return convert(fieldType, strings[fieldIndex]);
    }
}
