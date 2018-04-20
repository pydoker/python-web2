#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-08 15:41:27
# @Author  : pydoker (ldk_kdl@163.com)
# @Link    : ${link}
# @Version : $Id$
import logging
logging.basicConfig(level=logging.INFO)

import asyncio
import aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool  # __pool非公开变量，不应该被直接引用
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)  # logging.info('SQL: %s' % sql)
    global __pool
    # 从连接池获取一个connect，里面可以指定参数：如上。
    # yield from 将调用一个子协程（也就是在一个协程中调用另一个协程）
    # 并直接获得子协程的返回结果。
    with (yield from __pool) as conn:
        # 要想操作数据库需要创建游标。获取游标cursor
        cur = yield from conn.cursor(aiomysql.DictCursor)
        # 将输入的sql语句中的'？'替换为具体参数args
        # 通过游标cur操作execute()方法可以写入纯sql语句，对数据进行操作
        # 注意始终坚持使用带参数的SQL,而不是拼接的SQL字符串，防止注入攻击
        # execute（1,2）：1和2的关系类似于%格式化。
        # executemany(1，[])方法可以一次插入多条值，[]中放入多个参数。
        yield from cur.execute(sql.replace('?', '%s'), args or())
        # fetchone()方法每执行一次，游标会从表中的第一条数据移动到下一条数据的位置；
        # cur.scroll(0,'absolute')可以将游标定位到表中的第一条数据。
        if size:
                # fetchmany()可以获得多条数据，单需要指定数据的条数。
            rs = yield from cur.fetchmany(size)
        else:
                # fetchall获取所有记录
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        # 与select函数不同的是，cursor对象不返回结果集，而是通过rowcount返回结果数。
        return affected

# 这个函数是把查询字段计数替换成sql识别的？但没有看到有诸如此类的引用？
# 比如说：insert into'User'('password', 'email', 'name', 'id') values(?,?,?,?),四个问号


def create_args_string(num):
    lol = []
    for n in range(num):
        lol.append('?')
    return (','.join(lol))


# 定义field类，负责保存（数据库）表的字段名和字段类型
class Field(object):
        # 表的字段包含表名字、值类型、是否为主键和默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        # 返回表名字、字段类型和字段名
        return "<%s, %s:%s>" % (self.__class__.__name__, self.column_type, self.name)


# 定义数据库中五个存储类型
# 映射varchar的StringField
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


# 布尔类型不可以作为主键
class BooleanField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'Boolean', False, default)


# colum type是否可以自定义？先自定义看看。
class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'int', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'float', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# model只是一个基类，通过metaclass(ModelMetaclass)将具体的子类和user的映射信息读取出来
# class Model(dict, metaclass=ModelMetaclass):
# 定义Model的元类
# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类（继承ModelMetaclass)的子类实现的操作
# 读取具体子类（user)的映射信息
# 在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，
# 同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表明保存到__table__中
# 完成这些工作就可以在Model中定义各种数据库的操作方法
# metaclass是类的模板，所以必须从'type'类型派生。

class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
# 创造类的时候，排除对Model类的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
            tableName = attrs.get('__table__', None) or name
            logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('primary key not found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '%s' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        attrs['__select__'] = "select '%s', %s from '%s'" % (
            primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = "insert into '%s' (%s, '%s') values (%s)" % (tableName, ','.join(
            escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = "update '%s' set %s where '%s'=? " % (tableName, ','.join(
            map(lambda f: " '%s' = ?" % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = "delete from '%s' where '%s'=?" % (
            tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
