package hk.ust.model;

import java.io.Serializable;

/** TPC-H NATION table row. Only columns needed by Q10. PK: n_nationkey */
public record NationTuple(int nNationkey, String nName) implements Serializable {}
