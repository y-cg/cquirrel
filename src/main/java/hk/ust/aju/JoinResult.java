package hk.ust.aju;

import hk.ust.model.UpdateType;
import java.io.Serializable;

/**
 * A full join result for Q10, emitted when a lineitem tuple becomes alive (positive) or non-alive
 * (negative).
 *
 * <p>Corresponds to one row of: SELECT c_custkey, c_name, l_extendedprice*(1-l_discount) as
 * revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM lineitem JOIN orders JOIN customer
 * JOIN nation
 */
public record JoinResult(
    UpdateType type, // INSERT = positive delta, DELETE = negative delta
    long cCustkey,
    String cName,
    long revenue, // l_extendedprice * (1 - l_discount) in cents
    long cAcctbal, // in cents
    String nName,
    String cAddress,
    String cPhone,
    String cComment)
    implements Serializable {}
