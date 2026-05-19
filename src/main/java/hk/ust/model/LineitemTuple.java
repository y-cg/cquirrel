package hk.ust.model;

import java.io.Serializable;

/**
 * TPC-H LINEITEM table row. Only columns needed by Q10. PK: (l_orderkey, l_linenumber) FK:
 * l_orderkey -> orders.o_orderkey
 */
public record LineitemTuple(
    long lOrderkey,
    int lLinenumber,
    long lExtendedprice, // stored as cents (DECIMAL(15,2) * 100)
    long lDiscount, // stored as basis points: discount * 10000 (e.g., 0.05 -> 500)
    String lReturnflag)
    implements Serializable {
  /** Composite primary key for use in hash maps. */
  public record Key(long orderkey, int linenumber) implements Serializable {}

  public Key key() {
    return new Key(lOrderkey, lLinenumber);
  }

  /**
   * Compute revenue contribution: l_extendedprice * (1 - l_discount). Returns value in cents
   * (hundredths of currency unit).
   */
  public long revenueContribution() {
    // lExtendedprice is in cents, lDiscount is in basis points (x/10000)
    // revenue = price * (1 - discount) = price - price * discount / 10000
    return lExtendedprice - (lExtendedprice * lDiscount / 10000);
  }
}
