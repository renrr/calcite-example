package com.cherry.calcite.csv.table.scan;

import com.cherry.calcite.csv.table.flavor.CsvTranslatableTable;
import com.cherry.calcite.csv.table.rules.CsvProjectTableScanRule;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class CsvTableScan extends TableScan implements EnumerableRel {

    public final CsvTranslatableTable csvTable;
    public final int[] fields;

    public CsvTableScan(RelOptCluster cluster, RelOptTable table,
                           CsvTranslatableTable csvTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
        this.csvTable = csvTable;
        this.fields = fields;

        assert csvTable != null;
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new CsvTableScan(getCluster(), table, csvTable, fields);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("fields", Primitive.asList(fields));
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder =
                getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq)
                .multiplyBy(((double) fields.length + 2D)
                        / ((double) table.getRowType().getFieldCount() + 2D));
    }
    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(CsvProjectTableScanRule.INSTANCE);
    }



    /**
     * Creates a plan for this expression according to a calling convention.
     * (根据调用convention 为该表达式创建一个plan)
     * @param implementor
     * @param pref
     * @return
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(Expressions.call(table.getExpression(CsvTranslatableTable.class),
                                "project", implementor.getRootExpression(),
                        Expressions.constant(fields))));
    }
}
