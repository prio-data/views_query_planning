from collections import namedtuple

Statement = namedtuple("sql_statement",("type","args","kwargs"))

class MockQuery:
    def __init__(self):
        self.statements = []

    def select_from(self,*args,**kwargs):
        self.statements.append(Statement("from",args,kwargs))
        return self

    def add_columns(self,*args,**kwargs):
        self.statements.append(Statement("select",args,kwargs))
        return self

    def join(self,*args,**kwargs):
        self.statements.append(Statement("join",args,kwargs))
        return self

    def group_by(self,*args,**kwargs):
        self.statements.append(Statement("group_by",args,kwargs))
        return self

    def __str__(self):
        return (f"Query with the following ops:\n"
                f"{self.statements}"
            )

