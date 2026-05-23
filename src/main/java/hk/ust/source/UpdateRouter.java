package hk.ust.source;

import hk.ust.model.TupleUpdate;
import hk.ust.model.TupleUpdate.CustomerUpdate;
import hk.ust.model.TupleUpdate.LineitemUpdate;
import hk.ust.model.TupleUpdate.NationUpdate;
import hk.ust.model.TupleUpdate.OrdersUpdate;

/** Derives partition keys for tuple updates in partitioned AJU pipelines. */
public final class UpdateRouter {

  private UpdateRouter() {}

  public static long partitionKey(TupleUpdate update, OrderCustKeyIndex orderIndex) {
    return switch (update) {
      case NationUpdate u -> (long) u.tuple().nNationkey();
      case CustomerUpdate u -> u.tuple().cCustkey();
      case OrdersUpdate u -> u.tuple().oCustkey();
      case LineitemUpdate u -> orderIndex.custKeyForOrder(u.tuple().lOrderkey());
    };
  }

  public static int subtaskForKey(long partitionKey, int parallelism) {
    return Math.floorMod(Long.hashCode(partitionKey), parallelism);
  }
}
