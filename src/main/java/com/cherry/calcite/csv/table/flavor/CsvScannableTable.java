package com.cherry.calcite.csv.table.flavor;

import com.cherry.calcite.csv.converter.ArrayRowConverter;
import com.cherry.calcite.csv.enumerator.CsvEnumerator;
import com.cherry.calcite.csv.table.CsvTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import java.util.concurrent.atomic.AtomicBoolean;

public class CsvScannableTable extends CsvTable implements ScannableTable {


    public CsvScannableTable(Source source, RelProtoDataType relProtoDataType) {
        super(source, relProtoDataType);
    }


    //回调
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        int[] fields = CsvEnumerator.identityList(fieldTypes.size());
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(dataContext);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, null,
                        new ArrayRowConverter(fieldTypes, fields));
            }
        };
    }
}
