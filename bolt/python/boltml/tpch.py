# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pybolt import TpchTable as _TpchEnum


class TpchTable:
    """Descriptor for a TPCH table used by plan builders.

    Fields:
    - name: short table name (e.g., "nation")
    - boltEnum: pybolt.TpchTable enum value
    - colNames: ordered list of column names
    """

    name: str
    boltEnum: _TpchEnum
    colNames: list[str]


class Nation(TpchTable):
    name = "nation"
    boltEnum = _TpchEnum.nation
    colNames = ["nationkey", "name", "regionkey", "comment"]


class Region(TpchTable):
    name = "region"
    boltEnum = _TpchEnum.region
    colNames = ["regionkey", "name", "comment"]


class Part(TpchTable):
    name = "part"
    boltEnum = _TpchEnum.part
    colNames = [
        "partkey",
        "name",
        "mfgr",
        "brand",
        "type",
        "size",
        "container",
        "retailprice",
        "comment",
    ]


class Supplier(TpchTable):
    name = "supplier"
    boltEnum = _TpchEnum.supplier
    colNames = [
        "suppkey",
        "name",
        "address",
        "nationkey",
        "phone",
        "acctbal",
        "comment",
    ]


class Partsupp(TpchTable):
    name = "partsupp"
    boltEnum = _TpchEnum.partsupp
    colNames = ["partkey", "suppkey", "availqty", "supplycost", "comment"]


class Customer(TpchTable):
    name = "customer"
    boltEnum = _TpchEnum.customer
    colNames = [
        "custkey",
        "name",
        "address",
        "nationkey",
        "phone",
        "acctbal",
        "mktsegment",
        "comment",
    ]


class Orders(TpchTable):
    name = "orders"
    boltEnum = _TpchEnum.orders
    colNames = [
        "orderkey",
        "custkey",
        "orderstatus",
        "totalprice",
        "orderdate",
        "orderpriority",
        "clerk",
        "shippriority",
        "comment",
    ]


class Lineitem(TpchTable):
    name = "lineitem"
    boltEnum = _TpchEnum.lineitem
    colNames = [
        "orderkey",
        "partkey",
        "suppkey",
        "linenumber",
        "quantity",
        "extendedprice",
        "discount",
        "tax",
        "returnflag",
        "linestatus",
        "shipdate",
        "commitdate",
        "receiptdate",
        "shipinstruct",
        "shipmode",
        "comment",
    ]


_TABLE_MAP: dict[str, type[TpchTable]] = {
    "nation": Nation,
    "region": Region,
    "part": Part,
    "supplier": Supplier,
    "partsupp": Partsupp,
    "customer": Customer,
    "orders": Orders,
    "lineitem": Lineitem,
}


def tpchTable(name: str) -> type[TpchTable]:
    """Resolve a TPC-H table name to its ``TpchTable`` class.

    Pass the result to ``PlanBuilderFactory.tpchGenerator(table, ...)`` to
    build a TPC-H read plan, e.g.::

        df = DataFrame(
            BoltPlanBuilderFactory().tpchGenerator(tpchTable("orders")),
            executor=executor,
        )
    """
    try:
        return _TABLE_MAP[name]
    except KeyError:
        raise ValueError(
            f"Invalid tpch table: '{name}'. Valid tables are: {list(_TABLE_MAP.keys())}"
        )
