package hk.ust.source;

import hk.ust.model.*;
import java.time.LocalDate;

/**
 * Parses pipe-delimited TPC-H .tbl file lines into typed tuples.
 *
 * <p>TPC-H dbgen produces lines like:
 * 1|Customer#000000001|IVhzIApeRb|15|25-989-741-2988|711.56|BUILDING|comment...|
 *
 * <p>Note the trailing pipe delimiter - fields are split by '|' and the last empty element is
 * ignored.
 */
public final class TpchCsvParser {

  private TpchCsvParser() {}

  // =========================================================================
  // Nation: N_NATIONKEY|N_NAME|N_REGIONKEY|N_COMMENT|
  // We only need nationkey and name for Q10.
  // =========================================================================

  public static NationTuple parseNation(String line) {
    String[] fields = line.split("\\|", -1);
    return new NationTuple(
        Integer.parseInt(fields[0]), // n_nationkey
        fields[1] // n_name
        );
  }

  // =========================================================================
  // Customer: C_CUSTKEY|C_NAME|C_ADDRESS|C_NATIONKEY|C_PHONE|C_ACCTBAL|C_MKTSEGMENT|C_COMMENT|
  // Q10 uses all except c_mktsegment.
  // =========================================================================

  public static CustomerTuple parseCustomer(String line) {
    String[] fields = line.split("\\|", -1);
    return new CustomerTuple(
        Long.parseLong(fields[0]), // c_custkey
        fields[1], // c_name
        fields[2], // c_address
        Integer.parseInt(fields[3]), // c_nationkey
        fields[4], // c_phone
        parseCents(fields[5]), // c_acctbal as cents
        fields[7] // c_comment (skip mktsegment at [6])
        );
  }

  // =========================================================================
  // Orders: O_ORDERKEY|O_CUSTKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|
  //         O_ORDERPRIORITY|O_CLERK|O_SHIPPRIORITY|O_COMMENT|
  // Q10 uses orderkey, custkey, orderdate.
  // =========================================================================

  public static OrdersTuple parseOrders(String line) {
    String[] fields = line.split("\\|", -1);
    return new OrdersTuple(
        Long.parseLong(fields[0]), // o_orderkey
        Long.parseLong(fields[1]), // o_custkey
        LocalDate.parse(fields[4]) // o_orderdate (ISO format: YYYY-MM-DD)
        );
  }

  // =========================================================================
  // Lineitem: L_ORDERKEY|L_PARTKEY|L_SUPPKEY|L_LINENUMBER|L_QUANTITY|
  //           L_EXTENDEDPRICE|L_DISCOUNT|L_TAX|L_RETURNFLAG|L_LINESTATUS|
  //           L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|L_SHIPINSTRUCT|L_SHIPMODE|L_COMMENT|
  // Q10 uses orderkey, linenumber, extendedprice, discount, returnflag.
  // =========================================================================

  public static LineitemTuple parseLineitem(String line) {
    String[] fields = line.split("\\|", -1);
    return new LineitemTuple(
        Long.parseLong(fields[0]), // l_orderkey
        Integer.parseInt(fields[3]), // l_linenumber
        parseCents(fields[5]), // l_extendedprice as cents
        parseDiscount(fields[6]), // l_discount as basis points
        fields[8] // l_returnflag
        );
  }

  // =========================================================================
  // Numeric Parsing Utilities
  // =========================================================================

  /**
   * Parse a decimal string like "711.56" into cents (71156). TPC-H DECIMAL(15,2) always has exactly
   * 2 decimal places.
   */
  private static long parseCents(String value) {
    // Remove the decimal point and parse as integer cents.
    // Handle both "711.56" and potentially "711" (no decimal).
    boolean negative = value.startsWith("-");
    String abs = negative ? value.substring(1) : value;

    int dot = abs.indexOf('.');
    if (dot < 0) {
      return Long.parseLong(value) * 100;
    }
    String intPart = abs.substring(0, dot);
    String fracPart = abs.substring(dot + 1);
    // Pad or truncate to exactly 2 decimal places
    if (fracPart.length() == 1) fracPart = fracPart + "0";
    else if (fracPart.length() > 2) fracPart = fracPart.substring(0, 2);
    long cents = Long.parseLong(intPart) * 100 + Long.parseLong(fracPart);
    return negative ? -cents : cents;
  }

  /**
   * Parse a discount like "0.05" into basis points (500). This gives us integer arithmetic: price *
   * (10000 - discount_bp) / 10000.
   */
  private static long parseDiscount(String value) {
    // "0.05" -> 0.05 * 10000 = 500
    double d = Double.parseDouble(value);
    return Math.round(d * 10000);
  }
}
