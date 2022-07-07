# -*- coding: utf-8 -*-
# Author        ：   Nanmi
# Email         :    relakkes@gmail.com
# CreatedTime   ：   2022/7/7 13:40
# Desc          :    aiomysql事务版本

from typing import Dict, List, Optional, NoReturn

import aiomysql
from aiomysql import Connection


class AsyncDbTransaction(object):
    def __init__(self):
        self.conn: Connection = Optional[None]

    async def query(self, sql: str, *args) -> List[Dict]:
        """
        查询给定的SQL，返回列表
        :param sql:
        :param args:
        :return:
        """
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, *args)
            data = await cur.fetchall()
            if isinstance(data, tuple):
                data = list(data)
            return data

    async def get(self, sql: str, *args) -> Dict:
        """
        返回第一个结果
        :param sql:
        :param args:
        :return:
        """
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, *args)
            try:
                data = await cur.fetchall()
                return data[0]
            except Exception as e:
                return dict()

    async def execute(self, sql: str, *args) -> int:
        """
        需要更新、写入等操作的 excute 执行语句
        :param sql:
        :param args:
        :return:
        """
        async with self.conn.cursor() as cur:
            rows = await cur.execute(sql, *args)
            return rows

    async def is_in_table(self, table_name: str, field: str, value: str) -> bool:
        """
        表中是否含有指定数据
        :param table_name:
        :param field:
        :param value:
        :return:
        """
        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field, value)
        d = await self.get(sql)
        if d:
            return True
        return False

    async def item_to_table(self, table_name: str, item: Dict) -> int:
        """
        向指定的表明中插入一条记录
        :param table_name:
        :param item:
        :return:
        """
        fields = list(item.keys())
        values = list(item.values())

        # 处理insert中包含mysql关键字
        fields_list = list(map(lambda field: f'`{field}`', fields))
        fieldstr = ','.join(fields_list)
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)

        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, values)
            lastrowid = cur.lastrowid
            return lastrowid

    async def update_table(self, table_name: str, updates: Dict, field_where: str, value_where: str) -> int:
        """
        根据给定的字段、更新值，更新表中的一条数据
        :param table_name:
        :param updates:
        :param field_where:
        :param value_where:
        :return:
        """
        upsets = []
        values = []
        for k, v in updates.items():
            s = '`%s`=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where, value_where,
        )
        async with self.conn.cursor() as cur:
            rows = await cur.execute(sql, values)
            return rows

    async def begin(self, db_config: Dict) -> NoReturn:
        """
        开启一个mysql事物
        :param db_config:
        :return:
        """
        self.conn = await aiomysql.connect(**db_config, autocommit=False, loop=None)
        await self.conn.autocommit(False)
        await self.conn.begin()

    async def commit(self) -> NoReturn:
        """
        提交事物、并关闭链接
        :return:
        """
        await self.conn.commit()
        self.conn.close()

    async def rollback(self) -> NoReturn:
        """
        回滚事物
        :return:
        """
        await self.conn.rollback()
        self.conn.close()
