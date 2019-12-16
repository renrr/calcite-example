package com.cherry.calcite.csv;

import com.cherry.calcite.csv.table.flavor.CsvScannableTable;
import com.cherry.calcite.csv.table.CsvTable;
import com.cherry.calcite.csv.table.flavor.CsvTranslatableTable;
import com.cherry.calcite.csv.utils.CommonUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.Map;

/**
 *  Schema 映射到csv的文件目录，Schema中的每张表对应文件目录中的一个文件
 */
public class CsvSchema extends AbstractSchema {

    private final File directoryFile;

    private final CsvTable.Flavor flavor;

    private Map<String, Table> tableMap;


    public CsvSchema(File directoryFile, CsvTable.Flavor flavor) {
        this.directoryFile = directoryFile;
        this.flavor = flavor;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {

        Source baseSource = Sources.of(directoryFile);

        File[] files = loadSourceFiles();

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        for (File file : files) {
            Source source = Sources.of(file);
            Source sourceSansGz = source.trim(".gz");
            final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
            if (sourceSansCsv != null) {
                final Table table = createTable(source);
                builder.put(sourceSansCsv.relative(baseSource).path(), table);
            }

        }
        return builder.build();
    }

    private File[] loadSourceFiles() {

        File[] files = directoryFile.listFiles((dir, name) -> {
            final String nameSansGz = CommonUtils.trim(name, ".gz");
            return nameSansGz.endsWith(".csv")
                    || nameSansGz.endsWith(".json");
        });
        if (files == null) {
            System.out.println("directory " + directoryFile + " not found");
            files = new File[0];
        }
        return files;
    }

    private Table createTable(Source source) {
        switch (flavor) {
            case SCANNABLE:
                return new CsvScannableTable(source,null);
            case FILTERABLE:
            case TRANSLATABLE:
                return new CsvTranslatableTable(source,null);
            default:
                throw new AssertionError("Unknown flavor " + this.flavor);

        }
    }


}
