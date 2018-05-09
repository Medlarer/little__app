import asyncio
import aiomysql
import logging;logging.basicConfig(level=logging.INFO)

def log(sql,args=()):
    logging.info("SQL:%s" %sql)


@asyncio.coroutine
def create_pool(loop,**kwargs):
    logging.info("create database conncetion pool")
    global __pool
    __pool = yield from aiomysql.create_pool(
        host = kwargs.get("host","locathost"),
        port = kwargs.get("port",3306),
        user = kwargs["user"],
        password = kwargs["password"],
        db = kwargs["db"],
        chartset = kwargs.get("chartset","utf-8"),
        autocommit = kwargs.get("autocommit",True),
        maxsize = kwargs.get("maxsize",10),
        minsize = kwargs.get("minsize",1),
        loop = loop
    )

@asyncio.coroutine
def select(sql,args,size =None ):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.excute(sql.replace("?","%s"),args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info("rows returned:%s" %(len(rs),))
        return rs

@asyncio.coroutine
def excecute(sql,args,autocommit=True):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.excute(sql.replace("?","%s"),args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    l = []
    for n in range(num):
        l.append("?")
    return ",".join(l)

class Field(object):

     def __init__(self,name,column_type,primary_key,default):
         self.name = name
         self.column_type = column_type
         self.primary_key = primary_key
         self.default = default

     def __str__(self):
         return  '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self,name=None,primary_key = False,default=None,dd1="valchar(100)"):
        super().__init__(name,dd1,primary_key,default)

class BooleanField(Field):

    def __init__(self,name=None,default=None):
        super().__init__(name,"boolean",False,default)

class IntegerField(Field):

    def __init__(self,name=None,primary_key=False,default=0):
        super().__init__(name,"bigint",primary_key,default)

class FloatField(Field):

    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,"real",primary_key,default)

class TextField(Field):

    def __init__(self,name=None,default=None):
        super().__init__(name,"text",False,default)

class ModelMetaclass(type):

    def __new__(cls,name,bases,attrs,):
        if name == "Mode":
            return type.__new__(cls,name,bases,attrs)
        tableName = attrs.get("__table__",None) or name
        logging.info("found model:%s(table:%s)"%(name,tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info("found mapping:%s ==>%s"%(k,v))
                mappings[k] = v
                if v.primary_key:
                    #找到主键
                    if primaryKey:
                        raise StandardError("Duplicate primary key for field:%s"%k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandardError("primary key not found")
        for k in mappings.keys():
            attrs.pop(k)
            escaped_fields = list(map(lambda f:"`%s`"%f,fields))
            attrs["__mapping__"] = mappings #保存属性和列的映射关系
            attrs["__table__"] = tableName
            attrs["__primary_key__"] = primaryKey #主键属性名
            attrs["__fields__"] = fields #出主键的属性名
            attrs["__select__"] = "select `%s`,%s from `%s`"%(primaryKey,",".join(escaped_fields),tableName)
            attrs["__insert__"] = "insert into `%s`(%s,`%s`) value(%s)" %(tableName,".".join(escaped_fields),
                                                                          primaryKey,create_args_string(len(escaped_fields) + 1))
            attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
            attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
            return type.__new__(cls,name,bases,attrs)