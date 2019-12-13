package com.cherry.calcite.csv.table.flavor;

import com.cherry.calcite.csv.table.CsvTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Source;

import java.lang.reflect.Type;

public class CsvTranslatableTable extends CsvTable implements QueryableTable, TranslatableTable {

    public CsvTranslatableTable(Source source, RelProtoDataType relProtoDataType) {
        super(source, relProtoDataType);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String s) {
        return null;
    }

    @Override
    public Type getElementType() {
        return null;
    }

    @Override
    public Expression getExpression(SchemaPlus schemaPlus, String s, Class aClass) {
        return null;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return null;
    }
}
