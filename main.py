# -*- coding: utf-8 -*-
# @Time    : 2022/7/7 13:40
# @Author  : NanMI
# @Software: PyCharm
# @Desc    : 批量修改Mysql一个数据库中表、表字段的字符集

import asyncio
from typing import List, Dict, NoReturn

import config
from async_db import AsyncDbTransaction


class MysqlUpdateCharacterSet:

    def __init__(
            self,
            character_set: str = "utf8mb4",
            collate: str = "utf8mb4_general_ci"
    ) -> NoReturn:
        self.character_set = character_set
        self.collate = collate
        self.db: AsyncDbTransaction = AsyncDbTransaction()
        self.need_update_field_type = ["longtext", "text", "tinytext", "char", "varchar", "json", "mediumtext"]

    async def fetch_tables(self) -> List[str]:
        """
        查询数据库中所有的表名列表
        :return:
        """
        tables_name_list: List[str] = []
        takle_key = f"Tables_in_{config.DB_NANME}"
        records: List[Dict] = await self.db.query("show tables;")
        for item in records:
            table_name: str = item.get(takle_key)
            # 过滤掉视图
            if table_name.startswith("v_"):
                continue
            tables_name_list.append(item.get(takle_key))
        return tables_name_list

    async def moditfy_table_charset(self, tables: List[str]) -> NoReturn:
        """
        修改数据库中所有tables的字符集
        :param tables:
        :return:
        """
        for table_name in tables:
            await self.db.execute(f"alter table {table_name} row_format=Dynamic;")
            sql: str = f"ALTER TABLE {table_name} CONVERT TO CHARACTER SET {self.character_set} COLLATE {self.collate}"
            await self.db.execute(sql)
            await self.moditfy_fields_chaset(table_name)
            print(f"Table：{table_name} update done !")

    async def moditfy_fields_chaset(self, table_name: str) -> NoReturn:
        """
        修改数据库中一张表字段的字符集
        :param table_name:
        :return:
        """
        fileds_list = await self.db.query(f"desc {table_name};")
        fileds_name_list = [i.get('Field') for i in fileds_list]
        fileds_type_list = [i.get('Type') for i in fileds_list]
        for fname, ftype in zip(fileds_name_list, fileds_type_list):
            is_need_update: bool = self.check_current_filed_is_need_update(ftype)
            if is_need_update:
                sql: str = f"ALTER TABLE {table_name} CHANGE `{fname}` `{fname}` {ftype} CHARACTER SET {self.character_set} COLLATE {self.collate};"
                try:
                    await self.db.execute(sql)
                except Exception as e:
                    print(f"failed sql:", sql)

    def check_current_filed_is_need_update(self, current_filed: str) -> bool:
        """
        检查一个字段是否需要改变字符集
        :param current_filed:
        :return:
        """
        for need_filed_type in self.need_update_field_type:
            if need_filed_type.lower() in current_filed.lower():
                return True
        return False

    async def start(self) -> NoReturn:
        db_config = dict(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            db=config.DB_NANME,
            port=config.DB_PORT
        )
        await self.db.begin(db_config)
        all_tables: List[str] = await self.fetch_tables()
        await self.moditfy_table_charset(tables=all_tables)


async def main():
    mucs: MysqlUpdateCharacterSet = MysqlUpdateCharacterSet(
        character_set="utf8mb4",
        collate="utf8mb4_general_ci"
    )
    await mucs.start()
    await mucs.db.commit()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
