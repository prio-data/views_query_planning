
import logging
from unittest import TestCase
from sqlalchemy import Table, Column, MetaData, ForeignKey, Integer
from views_query_planning import join_network, QueryComposer

class TestQueryComposer(TestCase):
    def setUp(self):
        metadata = MetaData()

        Table("a",metadata,
                Column("id", Integer, primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("val", Integer))

        Table("b",metadata,
                Column("id", Integer, primary_key = True),
                Column("val", Integer),
                Column("fk_a", Integer, ForeignKey("a.id")),
                Column("fk_b", Integer, ForeignKey("c.id")))

        Table("c",metadata,
                Column("id", Integer, primary_key = True),
                Column("val", Integer))

        Table("d",metadata,
                Column("id", Integer, primary_key = True),
                Column("fk", Integer, ForeignKey("b.id")),
                Column("val", Integer))

        Table("e",metadata,
                Column("id", Integer, primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("fk", Integer, ForeignKey("d.id")))

        self.network = join_network(metadata.tables)

    def test_forwards_expression(self):

        compose = QueryComposer(self.network, "e", "t", "u")

        for t in ["a","b","c","d"]:
            expr = compose.expression(t, "val")
            self.assertTrue(expr.is_right())


    def test_backwards_expression(self):
        compose = QueryComposer(self.network, "a", "t", "u")

        self.assertTrue(compose.expression("b","val").is_right())
        self.assertTrue(compose.expression("c","val").is_left())
        self.assertTrue(compose.expression("d","val").is_right())

    def test_fails_with_wrong_column_name(self):
        compose = QueryComposer(self.network, "a", "t", "u")
        self.assertTrue(compose.expression("b","val").is_right())
        self.assertTrue(compose.expression("b","foo").is_left())

    def test_aggregates(self):
        compose = QueryComposer(self.network, "a", "t", "u")
        expression = compose.expression("b","val")
        self.assertIn("sum", expression.value)

    def test_does_not_aggregate(self):
        compose = QueryComposer(self.network, "e", "t", "u")
        expression = compose.expression("d","val")
        self.assertNotIn("sum", expression.value)

    def test_custom_agg_function(self):
        compose = QueryComposer(self.network, "a", "t", "u")
        for fn_name in ("avg", "sum", "min", "max"):
            expression = compose.expression("b","val",aggregation_function=fn_name)
            self.assertIn(fn_name, expression.value)

    def test_bad_loa_def(self):
        compose = QueryComposer(self.network, "a", "foo", "bar")
        e = compose.expression("b","val")
        self.assertTrue(e.is_left)

    def test_bad_aggregation_function(self):
        compose = QueryComposer(self.network, "a", "t", "u")
        a = compose.expression("b","val",aggregation_function="foobar")
        compose.expression("b","val",aggregation_function="avg")
        self.assertTrue(a.is_left)
        self.assertTrue(a.is_right)

    def test_one_one(self):
        metadata = MetaData()

        a = Table("a", metadata,
                Column("id", Integer, ForeignKey("b.id"), primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("val", Integer))

        b = Table("b", metadata,
                Column("id", Integer, primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("val", Integer))

        c = Table("c", metadata,
                Column("id", Integer, primary_key = True),
                Column("val", Integer),
                Column("fk", Integer, ForeignKey("a.id")))

        network = join_network(metadata.tables)

        a = QueryComposer(network, "a", "t", "u").expression("b","val")
        b = QueryComposer(network, "b", "t", "u").expression("a","val")
        self.assertNotIn("GROUP", a.value)
        self.assertNotIn("GROUP", b.value)

        c = QueryComposer(network, "a", "t", "u").expression("c","val")
        self.assertIn("GROUP", c.value)

    def test_proper_aliasing(self):
        compose = QueryComposer(self.network, "a", "t", "u")
        self.assertIn("val_avg", compose.expression("b", "val", aggregation_function = "avg").value)
        self.assertIn("val_max", compose.expression("b", "val", aggregation_function = "max").value)
