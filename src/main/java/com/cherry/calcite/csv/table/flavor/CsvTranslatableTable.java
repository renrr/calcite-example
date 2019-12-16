package com.cherry.calcite.csv.table.flavor;

import com.cherry.calcite.csv.enumerator.CsvEnumerator;
import com.cherry.calcite.csv.table.CsvTable;
import com.cherry.calcite.csv.table.scan.CsvTableScan;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于CSV文件的表
 */
public class CsvTranslatableTable extends CsvTable implements QueryableTable, TranslatableTable {

    public CsvTranslatableTable(Source source, RelProtoDataType relProtoDataType) {
        super(source, relProtoDataType);
    }


    public Enumerable<Object> project(final DataContext root,
                                      final int[] fields){
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, fieldTypes, fields);
            }
        };
    }


    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }


    /**
     * Generates an expression with which this table can be referenced in
     * generated code.
     *
     * @param schema    Schema
     * @param tableName Table name (unique within schema)
     * @param clazz     The desired collection class; for example {@code Queryable}.
     */
    @Override
    public Expression getExpression(SchemaPlus schema, String tableName,
                                    Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    /**
     * 将relOptTable表转换成RelNode
     */
    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final int[] fields = CsvEnumerator.identityList(fieldCount);
        return new CsvTableScan(toRelContext.getCluster(),relOptTable,this,fields);
    }
}
