<?php
declare(strict_types=1);

namespace icePHP;

class TableException extends \Exception
{
    //表名错误
    const TABLE_NAME_ERROR=3;

    //要执行的语句不能为空
    const EXECUTE_NULL=4;

    //要查询的语句不能为空
    const QUERY_NULL=5;

    //PDO语句参数绑定错误
    const PDO_BIND_ERROR=6;

    //锁表类型只能是READ/WRITE
    const LOCK_TYPE_ERROR=7;

    //允许多表查询情况下无法使用事务
    const TRANSACTION_IN_MULTI=8;

    //不允许单独的事务提交(缺少事务开始)
    const COMMIT_WITHOUT_BEGIN=9;

    //不允许单独的事务回滚(缺少事务开始)
    const ROLLBACK_WITHOUT_BEGIN=10;

    //未指明表名
    const MISS_TABLE_NAME=11;

    //关联关系格式错误
    const RELATION_TYPE_ERROR=12;

    //加载行记录对象时参数错误
    const RECORD_LOAD_ERROR=13;

    //字段不存在
    const FIELD_NOT_EXISTS=14;

    //要移除的记录无法识别
    const WANT_REMOVE_UNKNOWN=15;

    //映射Mongo时,主表字段不存在
    const MAP_MONGO_SOURCE_FIELD_NOT_EXISTS=16;

    //关联Mongo时,主表字段不存在
    const JOIN_MONGO_SOURCE_FIELD_NOT_EXISTS=17;

    //无法在行对象中找到指定的字段
    const FIELD_NOT_EXISTS_IN_ROW=18;

    //关联时,关联关系不足
    const JOIN_MORE_THAN_ON=19;

    //关联时,关联关系过多
    const JOIN_LESS_THAN_ON=20;

    //无法识别或不支持的SQL命令
    const SQL_COMMAD_UNKNOWN=21;

    //不应该到达这里
    const SHOULD_NOT_BE_HERE=22;

    //Update操作必须指定查询条件,不允许全表修改
    const MISS_CONDITION_IN_UPDATE=23;

    //Delete操作必须指定查询条件,不允许全表删除
    const MISS_CONDITION_IN_DELETE=24;

    //查询语句中不允许同时指定多个表名
    const MULTI_TABLE_IN_QUERY=25;

    //执行语句中不允许同时指定多个表名
    const MULTI_TABLE_IN_EXECUTE=26;

    //表名不匹配
    const TABLE_NAME_DIFFERENT=27;

    //尚未配置默认数据库(database|_default|read|database)
    const MISS_DEFAULT_DATABASE=28;
}