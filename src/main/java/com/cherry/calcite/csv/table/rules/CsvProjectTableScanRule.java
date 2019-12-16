package com.cherry.calcite.csv.table.rules;

import com.cherry.calcite.csv.table.scan.CsvTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class CsvProjectTableScanRule extends RelOptRule {

    public static final CsvProjectTableScanRule INSTANCE =
            new CsvProjectTableScanRule(RelFactories.LOGICAL_BUILDER);

    public CsvProjectTableScanRule(RelBuilderFactory relBuilderFactory) {
        super(
                operand(LogicalProject.class,
                        operand(CsvTableScan.class, none())),
                relBuilderFactory,
                "CsvProjectTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final CsvTableScan scan = call.rel(1);
        int[] fields = getProjectFields(project.getProjects());

        if (fields == null) {
            // Project contains expressions more complex than just field references.
            return;
        }
        call.transformTo(
                new CsvTableScan(
                        scan.getCluster(),
                        scan.getTable(),
                        scan.csvTable,
                        fields));
    }

    private int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];
        for (int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }
}
