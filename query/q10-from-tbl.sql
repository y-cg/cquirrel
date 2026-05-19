-- Load TPC-H data from .tbl files (pipe-delimited with trailing delimiter)
CREATE TABLE nation AS SELECT * FROM read_csv(getenv('TPCH_DATA_DIR') || '/nation.tbl', delim='|', header=false, columns={'n_nationkey': 'INTEGER', 'n_name': 'VARCHAR', 'n_regionkey': 'INTEGER', 'n_comment': 'VARCHAR'});
CREATE TABLE customer AS SELECT * FROM read_csv(getenv('TPCH_DATA_DIR') || '/customer.tbl', delim='|', header=false, columns={'c_custkey': 'INTEGER', 'c_name': 'VARCHAR', 'c_address': 'VARCHAR', 'c_nationkey': 'INTEGER', 'c_phone': 'VARCHAR', 'c_acctbal': 'DECIMAL(15,2)', 'c_mktsegment': 'VARCHAR', 'c_comment': 'VARCHAR'});
CREATE TABLE orders AS SELECT * FROM read_csv(getenv('TPCH_DATA_DIR') || '/orders.tbl', delim='|', header=false, columns={'o_orderkey': 'INTEGER', 'o_custkey': 'INTEGER', 'o_orderstatus': 'VARCHAR', 'o_totalprice': 'DECIMAL(15,2)', 'o_orderdate': 'DATE', 'o_orderpriority': 'VARCHAR', 'o_clerk': 'VARCHAR', 'o_shippriority': 'INTEGER', 'o_comment': 'VARCHAR'});
CREATE TABLE lineitem AS SELECT * FROM read_csv(getenv('TPCH_DATA_DIR') || '/lineitem.tbl', delim='|', header=false, columns={'l_orderkey': 'INTEGER', 'l_partkey': 'INTEGER', 'l_suppkey': 'INTEGER', 'l_linenumber': 'INTEGER', 'l_quantity': 'DECIMAL(15,2)', 'l_extendedprice': 'DECIMAL(15,2)', 'l_discount': 'DECIMAL(15,2)', 'l_tax': 'DECIMAL(15,2)', 'l_returnflag': 'VARCHAR', 'l_linestatus': 'VARCHAR', 'l_shipdate': 'DATE', 'l_commitdate': 'DATE', 'l_receiptdate': 'DATE', 'l_shipinstruct': 'VARCHAR', 'l_shipmode': 'VARCHAR', 'l_comment': 'VARCHAR'});

-- Q10: Returned Item Reporting
SELECT c_custkey, c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= date '1993-10-01'
  AND o_orderdate < date '1993-10-01' + interval '3' month
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;
