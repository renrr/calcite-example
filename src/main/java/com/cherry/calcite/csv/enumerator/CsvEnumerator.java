package com.cherry.calcite.csv.enumerator;

import java.util.Enumeration;

class CsvEnumerator<E> implements Enumeration<E> {

    @Override
    public boolean hasMoreElements() {
        return false;
    }

    @Override
    public E nextElement() {
        return null;
    }
}
