package com.cherry.calcite.csv.converter;

import com.cherry.calcite.csv.field.CsvFieldType;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.commons.lang3.time.FastDateFormat;


import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

/**
 * 行转换器
 *
 * @param <E>
 */
public abstract class RowConverter<E> {

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static{

        final TimeZone gmt = TimeZone.getTimeZone("GMT");

        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);

        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);

        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }



    public abstract E convertRow(String[] rows);


    protected Object convert(CsvFieldType fieldType, String string) {

        if (fieldType == null) {
            return string;
        }

        switch (fieldType) {

            case BOOLEAN:
                if (string.length() == 0) {
                    return null;
                }
                return Boolean.parseBoolean(string);
            case BYTE:
                if (string.length() == 0) {
                    return null;
                }
                return Byte.parseByte(string);
            case SHORT:
                if (string.length() == 0) {
                    return null;
                }
                return Short.parseShort(string);
            case INT:
                if (string.length() == 0) {
                    return null;
                }
                return Integer.parseInt(string);
            case LONG:
                if (string.length() == 0) {
                    return null;
                }
                return Long.parseLong(string);
            case FLOAT:
                if (string.length() == 0) {
                    return null;
                }
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.length() == 0) {
                    return null;
                }
                return Double.parseDouble(string);
            case DATE:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_DATE.parse(string);
                    return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                } catch (ParseException e) {
                    return null;
                }
            case TIME:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIME.parse(string);
                    return (int) date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case TIMESTAMP:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                    return date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case STRING:
            default:
                return string;
        }

    }

}
