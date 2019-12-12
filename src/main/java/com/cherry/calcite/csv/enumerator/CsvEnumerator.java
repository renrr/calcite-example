package com.cherry.calcite.csv.enumerator;

import au.com.bytecode.opencsv.CSVReader;
import com.cherry.calcite.csv.converter.ArrayRowConverter;
import com.cherry.calcite.csv.converter.RowConverter;
import com.cherry.calcite.csv.converter.SingleColumnRowConverter;
import com.cherry.calcite.csv.field.CsvFieldType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.IOException;
import java.util.ArrayList;



import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class CsvEnumerator<E> implements Enumerator<E> {


    private final CSVReader reader;
    private final String[] filterValues;
    private final AtomicBoolean cancelFlag;
    private final RowConverter<E> rowConverter;
    private E current;


    public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
                  List<CsvFieldType> fieldTypes) {
        this(source, cancelFlag, fieldTypes, identityList(fieldTypes.size()));
    }

    public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
                  List<CsvFieldType> fieldTypes, int[] fields) {
        //noinspection unchecked
        this(source, cancelFlag, false, null,
                (RowConverter<E>) converter(fieldTypes, fields));
    }

    public CsvEnumerator(Source source, AtomicBoolean cancelFlag, boolean stream,
                  String[] filterValues, RowConverter<E> rowConverter) {
        this.cancelFlag = cancelFlag;
        this.rowConverter = rowConverter;
        this.filterValues = filterValues;
        try {
            if (stream) {
                this.reader = new CsvStreamReader(source);
            } else {
                this.reader = openCsv(source);
            }
            this.reader.readNext(); // skip header row
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            outer:
            for (;;) {
                if (cancelFlag.get()) {
                    return false;
                }
                final String[] strings = reader.readNext();
                if (strings == null) {
                    if (reader instanceof CsvStreamReader) {
                        try {
                            Thread.sleep(CsvStreamReader.DEFAULT_MONITOR_DELAY);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        continue;
                    }
                    current = null;
                    reader.close();
                    return false;
                }
                if (filterValues != null) {
                    for (int i = 0; i < strings.length; i++) {
                        String filterValue = filterValues[i];
                        if (filterValue != null) {
                            if (!filterValue.equals(strings[i])) {
                                continue outer;
                            }
                        }
                    }
                }
                current = rowConverter.convertRow(strings);
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing CSV reader", e);
        }
    }

    private static RowConverter<?> converter(List<CsvFieldType> fieldTypes,
                                             int[] fields) {
        if (fields.length == 1) {
            final int field = fields[0];
            return new SingleColumnRowConverter(fieldTypes.get(field), field);
        } else {
            return new ArrayRowConverter(fieldTypes, fields);
        }
    }




    /**
     * 通过读取csv文件的第一行来推测表列的名称和类型
     *
     * @param javaTypeFactory
     * @param source
     * @param fieldTypes
     * @return
     */
    public static RelDataType deduceRowType(JavaTypeFactory javaTypeFactory,
                                            Source source, List<CsvFieldType> fieldTypes) {
        return deduceRowType(javaTypeFactory, source, fieldTypes, false);
    }

    private static RelDataType deduceRowType(JavaTypeFactory javaTypeFactory,
                                             Source source, List<CsvFieldType> fieldTypes, boolean stream) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();


        convert2RelDataType(javaTypeFactory, source, fieldTypes, types, names);

        if (names.isEmpty()) {
            names.add("line");
            types.add(javaTypeFactory.createSqlType(SqlTypeName.VARCHAR));
        }


        return javaTypeFactory.createStructType(Pair.zip(names, types));
    }

    private static void convert2RelDataType(JavaTypeFactory javaTypeFactory, Source source, List<CsvFieldType> fieldTypes, List<RelDataType> types, List<String> names) {
        try (CSVReader csvReader = openCsv(source)) {
            String[] strings = csvReader.readNext();

            for (String string : strings) {
                final String name;
                final CsvFieldType csvFieldType;
                final int colon = string.indexOf(":");
                if (colon > 0) {
                    name = string.substring(0, colon);
                    String stringType = string.substring(colon + 1);
                    csvFieldType = CsvFieldType.of(stringType);
                    if (csvFieldType == null) {
                        System.out.println("WARNING: Found unknown type: "
                                + stringType + " in file: " + source.path()
                                + " for column: " + name
                                + ". Will assume the type of column is string");
                    }
                } else {
                    name = string;
                    csvFieldType = null;
                }

                final RelDataType type;
                if (csvFieldType == null) {
                    type = javaTypeFactory.createSqlType(SqlTypeName.VARCHAR);
                } else {
                    type = csvFieldType.toType(javaTypeFactory);
                }

                names.add(name);
                types.add(type);
                if (fieldTypes != null) {
                    fieldTypes.add(csvFieldType);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static CSVReader openCsv(Source source) throws IOException {
        Objects.requireNonNull(source, "source");
        return new CSVReader(source.reader());
    }


    /**
     * 返回n长度的数组
     * @param n
     * @return
     */
    public static int [] identityList(int n) {
        int [] integers = new int[n];
        for (int i = 0; i < n ; i++) {
            integers[i] = i;
        }
        return integers;
    }
}
