<?php
declare(strict_types=1);

namespace icePHP;

class TableException extends \Exception
{
    //加载行记录对象时参数错误
    const RECORD_LOAD_ERROR=13;

    //字段不存在
    const FIELD_NOT_EXISTS=14;

    //无法在行对象中找到指定的字段
    const FIELD_NOT_EXISTS_IN_ROW=18;

}