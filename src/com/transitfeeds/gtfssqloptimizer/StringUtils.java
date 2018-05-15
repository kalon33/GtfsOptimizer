package com.transitfeeds.gtfssqloptimizer;

import java.util.List;
import java.util.Set;

public class StringUtils {
    public static <E> String join(Set<E> items, String glue) {
        int i = 0;
        String ret = "";

        for (E _val : items) {
            if (i++ > 0) {
                ret += glue;
            }
            ret += _val.toString();
        }

        return ret;
    }

    public static <E> String join(List<E> items, String glue) {
        int i = 0;
        String ret = "";

        for (E _val : items) {
            if (i++ > 0) {
                ret += glue;
            }
            ret += _val.toString();
        }

        return ret;
    }

}
