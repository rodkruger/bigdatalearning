package br.com.pucpr.posgraduacao.ia.bigdata.spark;

public class AnalysisUtils {

    public static final String COLSEPARATOR = ";";

    public static String getValue(String line, int col) {
        return line.split(COLSEPARATOR)[col];
    }

}
