package com.cherry.calcite.csv.flavor;

import com.cherry.calcite.csv.table.CsvTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

public class CsvScannableTable extends CsvTable implements ScannableTable {


    public CsvScannableTable(Source source, RelProtoDataType relProtoDataType) {
        super(source, relProtoDataType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        System.out.println("test");
        return null;
    }
}
