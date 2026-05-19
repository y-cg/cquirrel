package hk.ust.aju;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * In-memory state for a single relation in the AJU algorithm.
 *
 * <p>Maintains the data structures from Section 3.3 of the paper: - L(R): set of live tuples,
 * indexed by primary key - N(R): set of non-live tuples (non-leaf only), indexed by primary key -
 * s(t): counter = number of children on which t is alive - I(R, Rc): index mapping child PK values
 * to parent PK values
 *
 * <p>For Q10's chain structure, each non-leaf has exactly 1 child, so s(t) is either 0 or 1.
 *
 * @param <PK> primary key type of this relation
 * @param <T> tuple type of this relation
 * @param <FK> foreign key type (= PK of the child relation)
 */
public class RelationState<PK, T, FK> {

  // L(R): live tuples indexed by PK
  private final Map<PK, T> liveTuples = new HashMap<>();

  // N(R): non-live tuples indexed by PK (only used for non-leaf relations)
  private final Map<PK, T> nonLiveTuples = new HashMap<>();

  // s(t): alive-on-children counter per tuple PK (only for non-leaf)
  private final Map<PK, Integer> aliveCounter = new HashMap<>();

  // I(R, Rc): index from child's PK to set of this relation's PKs
  // that reference that child via FK.
  // Semantically: "given a child key, which tuples in R point to that child?"
  private final Map<FK, Set<PK>> childToParentIndex = new HashMap<>();

  private final boolean isLeaf;

  public RelationState(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  // =========================================================================
  // L(R) operations
  // =========================================================================

  public boolean isLive(PK key) {
    return liveTuples.containsKey(key);
  }

  public T getLive(PK key) {
    return liveTuples.get(key);
  }

  public void addLive(PK key, T tuple) {
    liveTuples.put(key, tuple);
  }

  public void removeLive(PK key) {
    liveTuples.remove(key);
  }

  // =========================================================================
  // N(R) operations
  // =========================================================================

  public boolean isNonLive(PK key) {
    return nonLiveTuples.containsKey(key);
  }

  public T getNonLive(PK key) {
    return nonLiveTuples.get(key);
  }

  public void addNonLive(PK key, T tuple) {
    nonLiveTuples.put(key, tuple);
  }

  public void removeNonLive(PK key) {
    nonLiveTuples.remove(key);
  }

  /** Move a tuple from N(R) to L(R). */
  public void promoteToLive(PK key) {
    T tuple = nonLiveTuples.remove(key);
    if (tuple != null) {
      liveTuples.put(key, tuple);
    }
  }

  /** Move a tuple from L(R) to N(R). */
  public void demoteToNonLive(PK key) {
    T tuple = liveTuples.remove(key);
    if (tuple != null) {
      nonLiveTuples.put(key, tuple);
    }
  }

  // =========================================================================
  // s(t) counter operations
  // =========================================================================

  public int getCounter(PK key) {
    return aliveCounter.getOrDefault(key, 0);
  }

  public void setCounter(PK key, int value) {
    aliveCounter.put(key, value);
  }

  public int incrementCounter(PK key) {
    int newVal = aliveCounter.merge(key, 1, Integer::sum);
    return newVal;
  }

  public int decrementCounter(PK key) {
    int newVal = aliveCounter.merge(key, -1, Integer::sum);
    return newVal;
  }

  public void removeCounter(PK key) {
    aliveCounter.remove(key);
  }

  // =========================================================================
  // I(R, Rc) index operations
  // =========================================================================

  /** Register that tuple with given PK references the child with given FK. */
  public void indexAdd(FK childKey, PK parentKey) {
    childToParentIndex.computeIfAbsent(childKey, k -> new HashSet<>()).add(parentKey);
  }

  /** Unregister the FK reference. */
  public void indexRemove(FK childKey, PK parentKey) {
    Set<PK> parents = childToParentIndex.get(childKey);
    if (parents != null) {
      parents.remove(parentKey);
      if (parents.isEmpty()) {
        childToParentIndex.remove(childKey);
      }
    }
  }

  /**
   * Find all tuples in this relation that reference the given child key. Returns empty set if none.
   */
  public Set<PK> getParentsOf(FK childKey) {
    return childToParentIndex.getOrDefault(childKey, Set.of());
  }

  // =========================================================================
  // Queries
  // =========================================================================

  public boolean contains(PK key) {
    return liveTuples.containsKey(key) || nonLiveTuples.containsKey(key);
  }

  public T get(PK key) {
    T t = liveTuples.get(key);
    return t != null ? t : nonLiveTuples.get(key);
  }

  public int liveCount() {
    return liveTuples.size();
  }

  public int nonLiveCount() {
    return nonLiveTuples.size();
  }
}
