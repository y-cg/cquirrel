package hk.ust.model;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * TPC-H ORDERS table row. Only columns needed by Q10. PK: o_orderkey FK: o_custkey ->
 * customer.c_custkey
 */
public record OrdersTuple(long oOrderkey, long oCustkey, LocalDate oOrderdate)
    implements Serializable {}
