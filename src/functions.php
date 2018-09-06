<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 生成一个标准表对象
 * 这是一个STable的快捷入口
 * @param string $name 表的别名
 * @param boolean $fileCache 是否要静态缓存
 * @return Table 表对象
 * @throws \Exception
 */
function table(string $name = '', bool $fileCache = false): Table
{
    //如果未提供表名,使用默认表
    if (!$name) {
        $name = config('database', '_defaultTable');
    }
    return Table::getInstance(trim($name), $fileCache);
}