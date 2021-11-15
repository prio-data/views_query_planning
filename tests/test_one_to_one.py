
from unittest import TestCase
from networkx.algorithms.shortest_paths import has_path
from sqlalchemy import Table, Column, ForeignKey, Integer, MetaData
from views_query_planning import join_network

class TestOneToOne(TestCase):
    def test_one_to_one_query(self):
        metadata = MetaData()
        a = Table("a",metadata,Column("pk",Integer,primary_key = True))
        b = Table("b",metadata,
                Column("pk",Integer,primary_key = True),
                Column("a_ref",Integer,ForeignKey("a.pk")))
        c = Table("c",metadata,
                Column("a_ref",Integer,ForeignKey("a.pk"), primary_key = True))

        d = Table("d",metadata,
                Column("a_ref",Integer,ForeignKey("a.pk"), primary_key = True),
                Column("b_ref",Integer,ForeignKey("b.pk"), primary_key = True),
                )

        network = join_network(metadata.tables)

        # 1:M table can only join in the O->M direction
        self.assertTrue(has_path(network, b, a))
        self.assertFalse(has_path(network, a, b))

        # 1:1 table can join both ways
        self.assertTrue(has_path(network, c, a))
        self.assertTrue(has_path(network, a, c))

        self.assertTrue(has_path(network, d, a))
        self.assertTrue(has_path(network, d, b))
        self.assertFalse(has_path(network, a, d))
        self.assertFalse(has_path(network, b, d))
