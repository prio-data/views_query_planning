
from unittest import TestCase
import re
import sqlalchemy as sa
from views_query_planning import QueryComposer, join_network

class TestOuter(TestCase):
    def test_outer_join(self):
        md = sa.MetaData()
        sa.Table("a", md,
                sa.Column("id", primary_key = True),
                sa.Column("t"),
                sa.Column("u"),
                sa.Column("val"),
                )
        sa.Table("b", md,
                sa.Column("id", primary_key = True),
                sa.Column("a_id", sa.ForeignKey("a.id")),
                sa.Column("val")
                )
        sa.Table("c", md,
                sa.Column("id", primary_key = True),
                sa.Column("t"),
                sa.Column("u"),
                sa.Column("b_id", sa.ForeignKey("b.id")),
                sa.Column("val")
                )

        outer = QueryComposer(join_network(md.tables), "a", "t", "u", outer = True)
        inner = QueryComposer(join_network(md.tables), "a", "t", "u", outer = False)

        self.assertEqual(len(re.findall("OUTER JOIN", outer.expression("c", "val").value)), 2)
        self.assertEqual(len(re.findall("OUTER JOIN", inner.expression("c", "val").value)), 0)
