package com.cherry.calcite.csv.converter;

import com.cherry.calcite.csv.field.CsvFieldType;

import java.util.List;

public class ArrayRowConverter extends RowConverter<Object[]> {

    private final CsvFieldType[] fieldTypes;
    private final int[] fields;
    private final boolean stream;

    public ArrayRowConverter(List<CsvFieldType> fieldTypes, int[] fields) {
        this.fieldTypes = fieldTypes.toArray(new CsvFieldType[0]);
        this.fields = fields;
        this.stream = false;
    }

    public ArrayRowConverter(List<CsvFieldType> fieldTypes, int[] fields, boolean stream) {
        this.fieldTypes = fieldTypes.toArray(new CsvFieldType[0]);
        this.fields = fields;
        this.stream = stream;
    }

    /**
     * 转换行
     * @param strings 每行中的数据
     * @return
     */
    @Override
    public Object[] convertRow(String[] strings) {
        if (stream) {
            return convertStreamRow(strings);
        } else {
            return convertNormalRow(strings);
        }
    }

    public Object[] convertNormalRow(String[] strings) {
        final Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int field = fields[i];
            objects[i] = convert(fieldTypes[field], strings[field]);
        }
        return objects;
    }

    public Object[] convertStreamRow(String[] strings) {
        final Object[] objects = new Object[fields.length + 1];
        objects[0] = System.currentTimeMillis();
        for (int i = 0; i < fields.length; i++) {
            int field = fields[i];
            objects[i + 1] = convert(fieldTypes[field], strings[field]);
        }
        return objects;
    }


}
