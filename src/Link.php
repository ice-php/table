<?php
declare(strict_types=1);

namespace icePHP;
/**
 * 抽象类,行类(SRow)与结果集类(SResult)的基类
 * @author Ice
 */
abstract class SLink implements \IteratorAggregate, \ArrayAccess
{
    // Table的名称
    protected $tableName;

    // 所有数据
    protected $data = [];

    /**
     * 表对象
     *
     * @var Table
     */
    protected $table;

    /**
     * 构造方法,保存Table名与初始数据
     *
     * @param string $tableName
     * @param array $data
     * @throws \Exception
     */
    protected function __construct(string $tableName, array $data)
    {
        // 保存表名
        $this->tableName = $tableName;

        // 保存数据
        $this->data = $data;

        // 保存表对象
        $this->table = self::makeTable($tableName);
    }

    /**
     * 处理关联关系
     *
     * @param mixed $relation
     *            可以是:
     *            *** '本表字段'
     *            *** '本表字段=关联表字段'
     *            ***    array('本表字段'=>'关联表字段')
     *            ***    array('本表字段','关联表字段')
     * @param $database string 数据库类型,当前只支持MYSQL
     * @return array(<本表字段>,<关联表字段>)
     * @throws \Exception
     */
    protected function relation($relation, string $database = 'mysql'): array
    {
        // 处理数据形式
        if (is_array($relation)) {
            foreach ($relation as $k => $v) {
                if (is_numeric($k)) {
                    return $v;
                }
                return [$k, $v];
            }
        }

        // 字符串形式
        if (!is_string($relation)) {
            throw new \Exception('Relation for link error:' . $relation);
        }

        // 分解关联键
        $matches = explode('=', $relation);

        // 如果只指明了一个字段,默认后一个字段为_id/id(表的主键)
        if (1 == count($matches)) {
            return [$relation, 'mongo' == $database ? '_id' : 'id'];
        }

        // 返回两个关联键
        return $matches;
    }

    /**
     * 一对一映射
     *
     * @param mixed $linkTableName 也可以是Table对象
     * @param mixed $relation 参考 relation 方法
     * @param mixed $fields
     * @return Result|Row
     */
    abstract public function map($linkTableName, $relation, $fields = '*');

    /**
     * 一对多映射
     *
     * @param mixed $linkTableName
     * @param mixed $relation 参考 relation 方法
     * @param mixed $fields
     * @param $where array
     * @param $orderBy mixed
     * @return Result|Row
     */
    abstract public function join($linkTableName, $relation, $fields = '*',
                                  $where = [], $orderBy = '');

    /**
     * 获取所有数据,以数组方式
     *
     * @return array
     */
    public function all(): array
    {
        return $this->data;
    }

    /**
     * 获取数据,以数组方式,这是个别名,方便ZF程序员使用
     */
    public function toArray(): array
    {
        return $this->all();
    }

    /**
     * 追加一个字段到字段列表
     *
     * @param mixed $fields
     * @param string $field
     * @return string|array
     * @throws \Exception
     */
    protected function append($fields, string $field)
    {
        // 转换为字段列表
        $fields = $this->table->createFields($fields);

        // 如果是*,不用附加了
        if ('*' == $fields) {
            return $fields;
        }

        // 分解为数组
        $fieldsArr = explode(',', $fields);

        // 如果要附加的字段已经在里面了
        if (in_array($field, $fieldsArr)) {
            return $fields;
        }

        // 附加一个字段
        $fieldsArr[] = $field;

        // 返回字段数组
        return $fieldsArr;
    }

    /**
     * 为一行数据,附加一些空的键值,用于MAP时
     *
     * @param array $row 行数据
     * @param string $fields 字段列表
     * @return array string
     */
    protected function appendNull(array $row, string $fields): array
    {
        // 这就不用附加了
        if ('*' == $fields) {
            return $row;
        }

        // 分解为数组
        $fieldsArr = explode(',', $fields);
        foreach ($fieldsArr as $k) {
            // 处理AS别名问题
            $matched = preg_match('/\sas\s(.*)/i', $k, $matches);
            if ($matched) {
                $k = $matches[1];
            }
            $k = trim($k, '`');

            // 如果本表已经有此字段,则不扩展
            if (!array_key_exists($k, $row)) {
                // 扩展值为空,因为没有对应
                $row[$k] = '';
            }
        }
        return $row;
    }

    /**
     * 生成关联表对象/数据模型对象
     * 可以是表名,可以是Table对象
     *
     * @param string|Table $table
     * @return Table|null
     * @throws \Exception
     */
    protected function makeTable($table): ?Table
    {
        // 如果是字符串,那么应该是表名,创建表对象
        if (is_string($table)) {
            return table($table);
        }

        // 如果已经是表对象,返回
        if ($table instanceof Table) {
            return $table;
        }

        return null;
    }

    /**
     * 以下四个方法用于聚合遍历
     */

    /**
     * @param int|string $offset
     * @return bool
     */
    public function offsetExists($offset): bool
    {
        return array_key_exists($offset, $this->data);
    }

    /**
     * 略
     * @param mixed $offset
     * @return mixed
     */
    public function offsetGet($offset)
    {
        return $this->data[$offset];
    }

    /**
     * 略
     * @param mixed $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value): void
    {
        $this->data[$offset] = $value;
    }

    /**
     * 略
     * @param mixed $offset
     */
    public function offsetUnset($offset): void
    {
        unset($this->data[$offset]);
    }

    /**
     * 判断本对象是否为空
     * @return boolean
     */
    public function isEmpty(): bool
    {
        return !($this->data and count($this->data));
    }

    /**
     * 判断本对象是否非空
     * @return boolean
     */
    public function isNotEmpty(): bool
    {
        return !($this->isEmpty());
    }
}