package com.cherry.calcite.csv;

import com.cherry.calcite.csv.table.CsvTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Locale;
import java.util.Map;


/**
 * 用户构建数据源的工厂，简单理解构建源数据库
 */
public class CsvSchemaFactory implements SchemaFactory {


    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

        final  String directory = (String) operand.get("directory");
        final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);

        File directoryFile = new File(directory);

        if (base != null && !directoryFile.isAbsolute()) {
            directoryFile = new File(base,directory);
        }

        String flavorName = (String) operand.get("flavor");
        CsvTable.Flavor flavor;
        if (flavorName == null) {
            flavor = CsvTable.Flavor.TRANSLATABLE;
        } else {
            flavor =  CsvTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
        }
        return new CsvSchema(directoryFile,flavor);

    }
}
