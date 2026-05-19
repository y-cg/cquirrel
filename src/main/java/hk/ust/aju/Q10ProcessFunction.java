package hk.ust.aju;

import hk.ust.model.*;
import hk.ust.model.TupleUpdate.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Core AJU algorithm implementation for TPC-H Q10.
 *
 * <p>Implements the Insert/Delete algorithms from Section 3.3 of the paper, specialized for Q10's
 * chain-shaped foreign-key DAG:
 *
 * <p>lineitem (root) -> orders -> customer -> nation (leaf)
 *
 * <p>Since the DAG is a chain (no branching), each non-leaf relation has exactly one child, so
 * |C(R)| = 1 for all non-leaf R. This means s(t) is either 0 or 1, and a tuple is alive iff s(t) =
 * 1.
 *
 * <p>Selection predicates are applied at entry (Section 2.3): updates that don't pass the filter
 * are discarded immediately.
 */
public class Q10ProcessFunction extends ProcessFunction<TupleUpdate, JoinResult> {

  // =========================================================================
  // State: one RelationState per table in the Q10 join chain
  // =========================================================================

  // nation (leaf): PK = Integer (n_nationkey), no FK to child
  private transient RelationState<Integer, NationTuple, Void> nationState;

  // customer: PK = Long (c_custkey), FK = Integer (c_nationkey -> nation)
  private transient RelationState<Long, CustomerTuple, Integer> customerState;

  // orders: PK = Long (o_orderkey), FK = Long (o_custkey -> customer)
  private transient RelationState<Long, OrdersTuple, Long> ordersState;

  // lineitem: PK = LineitemTuple.Key, FK = Long (l_orderkey -> orders)
  private transient RelationState<LineitemTuple.Key, LineitemTuple, Long> lineitemState;

  // =========================================================================
  // Selection predicate constants for Q10
  // =========================================================================

  private static final LocalDate ORDER_DATE_START = LocalDate.of(1993, 10, 1);
  private static final LocalDate ORDER_DATE_END = LocalDate.of(1994, 1, 1);
  private static final String RETURN_FLAG = "R";

  @Override
  public void open(OpenContext openContext) {
    nationState = new RelationState<>(true); // leaf
    customerState = new RelationState<>(false);
    ordersState = new RelationState<>(false);
    lineitemState = new RelationState<>(false); // root
  }

  // =========================================================================
  // Entry Point: dispatch by relation and update type
  // =========================================================================

  @Override
  public void processElement(TupleUpdate update, Context ctx, Collector<JoinResult> out) {
    switch (update) {
      case NationUpdate u -> {
        if (u.type() == UpdateType.INSERT) insertNation(u.tuple(), out);
        else deleteNation(u.tuple(), out);
      }
      case CustomerUpdate u -> {
        if (u.type() == UpdateType.INSERT) insertCustomer(u.tuple(), out);
        else deleteCustomer(u.tuple(), out);
      }
      case OrdersUpdate u -> {
        // Selection predicate: discard if order date outside range
        if (!passesOrdersFilter(u.tuple())) return;
        if (u.type() == UpdateType.INSERT) insertOrders(u.tuple(), out);
        else deleteOrders(u.tuple(), out);
      }
      case LineitemUpdate u -> {
        // Selection predicate: discard if returnflag != 'R'
        if (!passesLineitemFilter(u.tuple())) return;
        if (u.type() == UpdateType.INSERT) insertLineitem(u.tuple(), out);
        else deleteLineitem(u.tuple(), out);
      }
    }
  }

  // =========================================================================
  // Selection Predicates (Section 2.3)
  // =========================================================================

  private boolean passesOrdersFilter(OrdersTuple t) {
    LocalDate d = t.oOrderdate();
    return !d.isBefore(ORDER_DATE_START) && d.isBefore(ORDER_DATE_END);
  }

  private boolean passesLineitemFilter(LineitemTuple t) {
    return RETURN_FLAG.equals(t.lReturnflag());
  }

  // =========================================================================
  // NATION (leaf) - Insert/Delete
  // All tuples in a leaf are alive by definition (Lemma 4).
  // =========================================================================

  private void insertNation(NationTuple t, Collector<JoinResult> out) {
    int pk = t.nNationkey();
    nationState.addLive(pk, t);

    // Propagate up: find all customer tuples referencing this nation
    // and trigger Insert-Update on them.
    insertUpdateFromNation(pk, out);
  }

  private void deleteNation(NationTuple t, Collector<JoinResult> out) {
    int pk = t.nNationkey();
    if (!nationState.isLive(pk)) return;

    // Propagate up: all customers referencing this nation become non-alive
    deleteUpdateFromNation(pk, out);
    nationState.removeLive(pk);
  }

  // =========================================================================
  // CUSTOMER - Insert/Delete (Algorithm 1 from paper)
  // Child: nation (FK = c_nationkey)
  // =========================================================================

  private void insertCustomer(CustomerTuple t, Collector<JoinResult> out) {
    long pk = t.cCustkey();
    int fk = t.cNationkey(); // FK to nation

    // Update I(customer, nation): register this customer's reference to nation
    customerState.indexAdd(fk, pk);

    // Compute s(t): is the referenced nation tuple alive?
    int s = nationState.isLive(fk) ? 1 : 0;
    customerState.setCounter(pk, s);

    if (s == 1) {
      // Tuple is alive: add to L(customer), propagate up
      customerState.addLive(pk, t);
      insertUpdateFromCustomer(pk, out);
    } else {
      // Tuple is non-alive: add to N(customer)
      customerState.addNonLive(pk, t);
    }
  }

  private void deleteCustomer(CustomerTuple t, Collector<JoinResult> out) {
    long pk = t.cCustkey();
    int fk = t.cNationkey();

    if (customerState.isLive(pk)) {
      // Currently alive: propagate deletion upward, then remove
      deleteUpdateFromCustomer(pk, out);
      customerState.removeLive(pk);
    } else {
      customerState.removeNonLive(pk);
    }

    // Clean up index and counter
    customerState.indexRemove(fk, pk);
    customerState.removeCounter(pk);
  }

  // =========================================================================
  // ORDERS - Insert/Delete (Algorithm 1 from paper)
  // Child: customer (FK = o_custkey)
  // =========================================================================

  private void insertOrders(OrdersTuple t, Collector<JoinResult> out) {
    long pk = t.oOrderkey();
    long fk = t.oCustkey(); // FK to customer

    // Update I(orders, customer)
    ordersState.indexAdd(fk, pk);

    // Compute s(t): is the referenced customer alive?
    int s = customerState.isLive(fk) ? 1 : 0;
    ordersState.setCounter(pk, s);

    if (s == 1) {
      ordersState.addLive(pk, t);
      insertUpdateFromOrders(pk, out);
    } else {
      ordersState.addNonLive(pk, t);
    }
  }

  private void deleteOrders(OrdersTuple t, Collector<JoinResult> out) {
    long pk = t.oOrderkey();
    long fk = t.oCustkey();

    if (ordersState.isLive(pk)) {
      deleteUpdateFromOrders(pk, out);
      ordersState.removeLive(pk);
    } else {
      ordersState.removeNonLive(pk);
    }

    ordersState.indexRemove(fk, pk);
    ordersState.removeCounter(pk);
  }

  // =========================================================================
  // LINEITEM (root) - Insert/Delete (Algorithm 1 from paper)
  // Child: orders (FK = l_orderkey)
  // =========================================================================

  private void insertLineitem(LineitemTuple t, Collector<JoinResult> out) {
    LineitemTuple.Key pk = t.key();
    long fk = t.lOrderkey(); // FK to orders

    // Update I(lineitem, orders)
    lineitemState.indexAdd(fk, pk);

    // Compute s(t): is the referenced orders tuple alive?
    int s = ordersState.isLive(fk) ? 1 : 0;
    lineitemState.setCounter(pk, s);

    if (s == 1) {
      // Lineitem is alive -> it contributes to the query result!
      lineitemState.addLive(pk, t);
      // Emit positive join result
      JoinResult result = buildJoinResult(t, UpdateType.INSERT);
      if (result != null) out.collect(result);
    } else {
      lineitemState.addNonLive(pk, t);
    }
  }

  private void deleteLineitem(LineitemTuple t, Collector<JoinResult> out) {
    LineitemTuple.Key pk = t.key();
    long fk = t.lOrderkey();

    if (lineitemState.isLive(pk)) {
      // Emit negative join result before removing
      JoinResult result = buildJoinResult(t, UpdateType.DELETE);
      if (result != null) out.collect(result);
      lineitemState.removeLive(pk);
    } else {
      lineitemState.removeNonLive(pk);
    }

    lineitemState.indexRemove(fk, pk);
    lineitemState.removeCounter(pk);
  }

  // =========================================================================
  // Insert-Update: propagate "became alive" upward (Algorithm 2 from paper)
  //
  // When a tuple in a child relation becomes alive, parent tuples that
  // reference it may now also become alive (their s(t) increments to |C(R)|=1).
  // =========================================================================

  /** A nation tuple became alive. Find customers referencing it and update them. */
  private void insertUpdateFromNation(int nationKey, Collector<JoinResult> out) {
    // I(customer, nation) gives us all customers with c_nationkey = nationKey
    Set<Long> customerKeys = customerState.getParentsOf(nationKey);
    for (long custKey : List.copyOf(customerKeys)) {
      int newS = customerState.incrementCounter(custKey);
      if (newS == 1) {
        // Customer just became alive
        customerState.promoteToLive(custKey);
        insertUpdateFromCustomer(custKey, out);
      }
    }
  }

  /** A customer tuple became alive. Find orders referencing it and update them. */
  private void insertUpdateFromCustomer(long custKey, Collector<JoinResult> out) {
    // I(orders, customer) gives us all orders with o_custkey = custKey
    Set<Long> orderKeys = ordersState.getParentsOf(custKey);
    for (long orderKey : List.copyOf(orderKeys)) {
      int newS = ordersState.incrementCounter(orderKey);
      if (newS == 1) {
        // Order just became alive
        ordersState.promoteToLive(orderKey);
        insertUpdateFromOrders(orderKey, out);
      }
    }
  }

  /** An orders tuple became alive. Find lineitems referencing it and update them. */
  private void insertUpdateFromOrders(long orderKey, Collector<JoinResult> out) {
    // I(lineitem, orders) gives us all lineitems with l_orderkey = orderKey
    Set<LineitemTuple.Key> lineitemKeys = lineitemState.getParentsOf(orderKey);
    for (LineitemTuple.Key liKey : List.copyOf(lineitemKeys)) {
      int newS = lineitemState.incrementCounter(liKey);
      if (newS == 1) {
        // Lineitem just became alive -> emit join result
        lineitemState.promoteToLive(liKey);
        LineitemTuple li = lineitemState.getLive(liKey);
        if (li != null) {
          JoinResult result = buildJoinResult(li, UpdateType.INSERT);
          if (result != null) out.collect(result);
        }
      }
    }
  }

  // =========================================================================
  // Delete-Update: propagate "became non-alive" upward (Algorithm 4 from paper)
  //
  // When a tuple in a child relation becomes non-alive (or is deleted),
  // parent tuples may lose their alive status (s(t) drops below |C(R)|=1).
  // =========================================================================

  /**
   * A nation tuple is being deleted/becoming non-alive. All customers referencing it lose their
   * alive status.
   */
  private void deleteUpdateFromNation(int nationKey, Collector<JoinResult> out) {
    Set<Long> customerKeys = customerState.getParentsOf(nationKey);
    for (long custKey : List.copyOf(customerKeys)) {
      if (customerState.isLive(custKey)) {
        customerState.decrementCounter(custKey);
        // s dropped to 0 -> customer becomes non-alive
        customerState.demoteToNonLive(custKey);
        deleteUpdateFromCustomer(custKey, out);
      } else {
        customerState.decrementCounter(custKey);
      }
    }
  }

  /** A customer is becoming non-alive. Orders referencing it lose alive status. */
  private void deleteUpdateFromCustomer(long custKey, Collector<JoinResult> out) {
    Set<Long> orderKeys = ordersState.getParentsOf(custKey);
    for (long orderKey : List.copyOf(orderKeys)) {
      if (ordersState.isLive(orderKey)) {
        ordersState.decrementCounter(orderKey);
        ordersState.demoteToNonLive(orderKey);
        deleteUpdateFromOrders(orderKey, out);
      } else {
        ordersState.decrementCounter(orderKey);
      }
    }
  }

  /** An orders tuple is becoming non-alive. Lineitems referencing it lose alive status. */
  private void deleteUpdateFromOrders(long orderKey, Collector<JoinResult> out) {
    Set<LineitemTuple.Key> lineitemKeys = lineitemState.getParentsOf(orderKey);
    for (LineitemTuple.Key liKey : List.copyOf(lineitemKeys)) {
      if (lineitemState.isLive(liKey)) {
        lineitemState.decrementCounter(liKey);
        // Emit negative delta before demoting
        LineitemTuple li = lineitemState.getLive(liKey);
        if (li != null) {
          JoinResult result = buildJoinResult(li, UpdateType.DELETE);
          if (result != null) out.collect(result);
        }
        lineitemState.demoteToNonLive(liKey);
      } else {
        lineitemState.decrementCounter(liKey);
      }
    }
  }

  // =========================================================================
  // Join Result Construction
  //
  // When a lineitem at the root becomes alive, we walk DOWN the chain
  // to assemble the full join tuple needed for Q10's output.
  // =========================================================================

  private JoinResult buildJoinResult(LineitemTuple li, UpdateType type) {
    // Walk down: lineitem -> orders -> customer -> nation
    OrdersTuple order = ordersState.getLive(li.lOrderkey());
    if (order == null) return null;

    CustomerTuple customer = customerState.getLive(order.oCustkey());
    if (customer == null) return null;

    NationTuple nation = nationState.getLive(customer.cNationkey());
    if (nation == null) return null;

    return new JoinResult(
        type,
        customer.cCustkey(),
        customer.cName(),
        li.revenueContribution(),
        customer.cAcctbal(),
        nation.nName(),
        customer.cAddress(),
        customer.cPhone(),
        customer.cComment());
  }
}
