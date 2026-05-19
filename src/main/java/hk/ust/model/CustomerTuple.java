package hk.ust.model;

import java.io.Serializable;

/**
 * TPC-H CUSTOMER table row. Only columns needed by Q10. PK: c_custkey FK: c_nationkey ->
 * nation.n_nationkey
 */
public record CustomerTuple(
    long cCustkey,
    String cName,
    String cAddress,
    int cNationkey,
    String cPhone,
    long cAcctbal, // stored as cents (DECIMAL(15,2) * 100) for performance
    String cComment)
    implements Serializable {}
