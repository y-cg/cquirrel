package hk.ust.model;

import java.io.Serializable;

/**
 * A tagged update to one of the 4 relations in Q10. Each record in the stream specifies: (1) insert
 * or delete, (2) which relation, (3) the tuple attributes.
 */
public sealed interface TupleUpdate extends Serializable {
  UpdateType type();

  record NationUpdate(UpdateType type, NationTuple tuple) implements TupleUpdate {}

  record CustomerUpdate(UpdateType type, CustomerTuple tuple) implements TupleUpdate {}

  record OrdersUpdate(UpdateType type, OrdersTuple tuple) implements TupleUpdate {}

  record LineitemUpdate(UpdateType type, LineitemTuple tuple) implements TupleUpdate {}
}
