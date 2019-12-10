package com.cherry.calcite.csv.table;

import com.cherry.calcite.csv.field.CsvFieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * calcite 中对应表的基类
 */
public abstract class CsvTable extends AbstractTable {

    protected final Source source;
    protected final RelProtoDataType relProtoDataType;
    protected List<CsvFieldType> fieldTypes;


    public CsvTable(Source source, RelProtoDataType relProtoDataType) {
        this.source = source;
        this.relProtoDataType = relProtoDataType;
    }

    /**
     *  getConnection 会回调该方法
     * @param relDataTypeFactory
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {

        if (relProtoDataType != null) {
            relProtoDataType.apply(relDataTypeFactory);
        }

        if (fieldTypes == null) {
            fieldTypes = new ArrayList<>();

        }

        return null;
    }

    /**
     * 对表操作的维度
     */
    public enum Flavor{
        SCANNABLE,
        FILTERABLE,
        TRANSLATABLE
    }
}
