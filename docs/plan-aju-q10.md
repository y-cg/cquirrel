# Plan: Implement AJU Algorithm for TPC-H Q10

## Context

The SIGMOD'20 paper "Maintaining Acyclic Foreign-Key Joins under Updates" (Wang & Yi, HKUST) presents the AJU algorithm for incrementally maintaining query results under insertions/deletions with O(lambda) amortized cost. We implement a Flink **streaming** job that processes updates to the 4 tables in Q10 and incrementally maintains the query result using the paper's algorithm.

Q10 involves: `lineitem JOIN orders JOIN customer JOIN nation` with filters, GROUP BY, SUM aggregation, and top-20.

---

## Q10's Foreign-Key DAG (chain, no branching)

```
lineitem (root, in-degree 0)
   | FK: l_orderkey -> o_orderkey
orders
   | FK: o_custkey -> c_custkey
customer
   | FK: c_nationkey -> n_nationkey
nation (leaf, out-degree 0)
```

Since it's a simple chain: no unconstrained pairs, no assertion keys, no auxiliary attributes needed. This is the simplest case of the AJU algorithm.

Selection predicates (Section 2.3 - discard non-passing updates):

- lineitem: `l_returnflag = 'R'`
- orders: `o_orderdate IN [1993-10-01, 1994-01-01)`

---

## File Structure

```
src/main/java/hk/ust/
├── AjuStreamJob.java                 # Streaming entry point
├── model/
│   ├── UpdateType.java               # Enum: INSERT, DELETE
│   ├── TupleUpdate.java              # Sealed interface for typed updates
│   ├── LineitemTuple.java            # Record: l_orderkey, l_linenumber, l_extendedprice, l_discount, l_returnflag
│   ├── OrdersTuple.java             # Record: o_orderkey, o_custkey, o_orderdate
│   ├── CustomerTuple.java           # Record: c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment
│   └── NationTuple.java             # Record: n_nationkey, n_name
├── source/
│   ├── TpchCsvParser.java           # Parse pipe-delimited .tbl lines into tuples
│   └── TpchUpdateSource.java        # SourceFunction: bulk-load .tbl files as INSERTs, then process update files
├── aju/
│   ├── Q10ProcessFunction.java      # Core AJU algorithms (Insert/Delete + Insert-Update/Delete-Update)
│   ├── RelationState.java           # Per-relation state: L(R), N(R), s(t), I(R, Rc)
│   └── JoinResult.java              # Full join result tuple emitted at root
├── aggregate/
│   ├── Q10Aggregator.java           # Incremental SUM(revenue) per c_custkey group
│   └── TopKMaintainer.java          # Top-20 by revenue (TreeMap-based)
└── sink/
    └── DeltaSink.java               # Print deltas to stdout
```

---

## Core Algorithm (Section 3.3, simplified for chain)

### Data Structures per relation R

| Structure  | Purpose                                                      | Key                  |
| ---------- | ------------------------------------------------------------ | -------------------- |
| `L(R)`     | Live tuples                                                  | PK(R)                |
| `N(R)`     | Non-live tuples (non-leaf only)                              | PK(R)                |
| `s(t)`     | # children on which t is alive (stored with N(R) entry)      | PK(R)                |
| `I(R, Rc)` | Parent-child index: find tuples in R by their FK to child Rc | PK(Rc) -> Set<PK(R)> |

For Q10's chain, each non-leaf has exactly 1 child, so `s(t) in {0, 1}` and `|C(R)| = 1`.

### Insert(t, R)

1. Apply selection predicate; discard if fails
2. If R is leaf (nation): add to L(R), propagate up via Insert-Update
3. If R is non-leaf:
   - Look up child relation's L(Rc) for matching FK -> compute s(t)
   - Update I(R, Rc) index
   - If s(t) = 1: t is alive -> add to L(R), propagate up via Insert-Update
   - Else: add to N(R)

### Insert-Update(t, R, join_result)

1. Add t to L(R) (move from N(R) if applicable)
2. If R is root (lineitem): emit join_result to aggregator as positive delta
3. Else: find parents via I(Rp, R) with key PK(R)
   - For each parent tp: s(tp)++
   - If s(tp) reaches |C(Rp)| = 1: tp becomes alive -> recurse Insert-Update(tp, Rp, ...)

### Delete(t, R) and Delete-Update (symmetric)

Reverse of insert: decrement s(tp), move alive->non-alive if s drops below threshold, emit negative deltas at root.

### Join Result Construction at Root

When a lineitem tuple becomes alive, walk DOWN the chain to assemble the full output:

```
lineitem -> L(orders)[l_orderkey] -> L(customer)[o_custkey] -> L(nation)[c_nationkey]
```

Compute `revenue_contribution = l_extendedprice * (1 - l_discount)`.

---

## Aggregation (O(1) per update for SUM)

Since SUM supports additive inverses:

- Positive delta: `revenueByCustomer[c_custkey] += contribution`
- Negative delta: `revenueByCustomer[c_custkey] -= contribution`

No binary tree needed (binary tree is only for MAX/MIN per the paper).

---

## Key Design Decisions

| Decision           | Choice                                         | Rationale                                      |
| ------------------ | ---------------------------------------------- | ---------------------------------------------- |
| State storage      | In-memory HashMap                              | Matches paper's eval; fastest; prototype first |
| Parallelism        | Single partition (keyBy constant)              | Chain requires co-located state                |
| Tuple types        | Java records                                   | Immutable, auto equals/hashCode                |
| Numeric precision  | long (cents) for aggregation                   | Avoid BigDecimal overhead on hot path          |
| Initial load order | Bottom-up (nation, customer, orders, lineitem) | Avoids accumulating tuples in N(R)             |

---

## Implementation Phases

### Phase 1: Data Model + Source

- Create model records and TpchCsvParser
- Create TpchUpdateSource (read .tbl files, emit INSERT stream)
- Wire up AjuStreamJob to verify source works

### Phase 2: Core AJU State + Algorithms

- Implement RelationState with HashMap-based L/N/s/I
- Implement Q10ProcessFunction with Insert/Delete/Insert-Update/Delete-Update
- Unit test with small hand-crafted data

### Phase 3: Aggregation + Output

- Implement Q10Aggregator (incremental SUM per group)
- Implement TopKMaintainer
- Implement DeltaSink

### Phase 4: End-to-End Verification

- Load SF-0.01 data, run the streaming job
- Compare final aggregated results against DuckDB `SELECT` on Q10
- Verify correctness of incremental updates (insert some tuples, delete some, check)

---

## Verification

1. **Unit test**: Manually construct a small 4-table dataset (5 nations, 10 customers, 20 orders, 50 lineitems). Insert all, verify join results match expected.
2. **Batch comparison**: Run Q10 on SF-0.01 via DuckDB (already in devenv). Compare AJU's final materialized result.
3. **Incremental correctness**: After initial load, apply a few DELETE+INSERT updates. Verify deltas are correct by re-running the full query and diffing.
