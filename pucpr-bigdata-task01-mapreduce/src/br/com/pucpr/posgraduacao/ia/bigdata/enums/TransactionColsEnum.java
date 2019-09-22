package br.com.pucpr.posgraduacao.ia.bigdata.enums;

/**
 * Columns for the transactions file
 */
public enum TransactionColsEnum {

    COUNTRY(0),
    YEAR(1),
    COMMCODE(2),
    COMMODITY(3),
    FLOW(4),
    TRADEUSD(5),
    WEIGHTKG(6),
    QUANTITYNAME(7),
    QUANTITY(8),
    CATEGORY(9);

    private int value;

    private TransactionColsEnum(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

} // end TransactionColsEnum
