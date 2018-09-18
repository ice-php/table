<?php
declare(strict_types=1);

namespace icePHP;

/**
 * Row 返回的行对象
 * @author Ice
 *行对象也有一些常用方法
 **** all 返回行数据(一维数组)
 **** map 一对一映射
 **** join 一对多映射
 */
class Row implements \IteratorAggregate, \ArrayAccess, \JsonSerializable
{
    use LinkTrait;

    /**
     * 给定表名和一行数据,创建一个行对象
     *
     * @param string|Table $table
     * @param array|null $data
     */
    public function __construct($table, ? array $data = null)
    {
        self::construct($table, $data);
    }

    /**
     * 一对一映射
     *
     * @param mixed $linkTableName
     * @param mixed $relation 参考 _relation 方法
     * @param mixed $fields
     * @param $where mixed
     * @return Row
     */
    public function map($linkTableName, $relation, $fields = '*', $where = []): Row
    {
        // 生成关联表模型对象
        $table = $this->makeTable($linkTableName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation);

        // 如果本表关联字段不存在
        if (!array_key_exists($field, $this->data)) {
            return $this;
        }

        // 生成本表关联字段的所有值
        $key = $this->data[$field];

        if (is_null($key)) {
            $linkRow = false;
        } else {
            // 生成关联表查询条件
            $where[$linkField] = $key;

            // 关联表查询结果
            $linkRow = $table->row($this->append($fields, $linkField), $where);
        }

        // 如果未能对应
        if (!$linkRow) {
            $fields = $this->table->createFields($fields);
            $this->data = $this->appendNull($this->data, $fields);
            return $this;
        }

        // 这里是对应的数据
        $linkRow = $linkRow->toArray();

        // 处理每一个关联的字段
        foreach ($linkRow as $k => $v) {
            // 如果本表已经有此字段,则不扩展
            if (!array_key_exists($k, $this->data)) {
                $this->data[$k] = $v;
            }
        }

        return $this;
    }

    /**
     * 一对一映射到Mongo上
     *
     * @param string $linkMongoName
     * @param mixed $relation
     * @param array $fields
     * @return Row
     */
    public function mapMongo($linkMongoName, $relation, array $fields = []): Row
    {
        if (empty($fields)) {
            return $this;
        }

        // 生成关联表模型对象
        $mongo = mongo($linkMongoName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation, 'mongo');

        // 如果本表关联字段不存在
        if (!array_key_exists($field, $this->data)) {
            trigger_error('映射Mongo时,主表字段不存在:' . $field, E_USER_ERROR);
        }

        // 生成本表关联字段的所有值
        $key = $this->data[$field];

        if (is_null($key)) {
            $linkRow = false;
        } else {
            // 生成关联表查询条件
            $where = [
                $linkField => $key
            ];

            // 关联表查询结果
            $fields[$linkField] = true;
            if (!array_key_exists('_id', $fields)) {
                $fields['_id'] = false;
            }

            // 生成查询游标
            $linkRow = $mongo->getCollection()->findOne($where, $fields);
        }

        // 如果未能对应
        if (!$linkRow) {
            foreach ($fields as $k => $v) {
                // 如果本表已经有此字段,则不扩展
                if ($v and !array_key_exists($k, $this->data)) {
                    // 扩展值为空,因为没有对应
                    $this->data[$k] = null;
                }
            }
            return $this;
        }

        // 处理每一个关联的字段
        foreach ($linkRow as $k => $v) {
            // 如果本表已经有此字段,则不扩展
            if (!array_key_exists($k, $this->data)) {
                $this->data[$k] = $v;
            }
        }

        return $this;
    }

    /**
     * 一对多映射
     *
     * @param mixed $linkTableName
     * @param mixed $relation 参考 _relation 方法
     * @param mixed $fields
     * @param mixed $where
     * @param mixed $orderBy
     * @return Row
     */
    public function join($linkTableName, $relation, $fields = '*', $where = [], $orderBy = ''): Row
    {
        // 生成关联表模型对象
        $table = $this->makeTable($linkTableName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation);

        // 如果本表关联字段不存在
        if (!array_key_exists($field, $this->data)) {
            return $this;
        }

        // 生成本表关联字段的所有值
        $key = $this->data[$field];

        // 生成关联表查询条件
        $where[$linkField] = $key;

        // 关联表查询结果
        $linkResult = $table->select($this->append($fields, $linkField), $where, $orderBy);

        $this->data[$table->alias()] = $linkResult;

        return $this;
    }

    /**
     * 一对多关联到Mongo上
     *
     * @param string $linkMongoName
     * @param mixed $relation
     * @param array $fields
     * @param mixed $where
     * @param mixed $sort
     * @return Row
     */
    public function joinMongo($linkMongoName, $relation, array $fields = [], $where = [], $sort = []): Row
    {
        if (empty($fields)) {
            return $this;
        }

        // 生成关联表模型对象
        $mongo = mongo($linkMongoName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation, 'mongo');

        // 如果本表关联字段不存在
        if (!array_key_exists($field, $this->data)) {
            trigger_error('关联Mongo时,主表字段不存在:' . $field, E_USER_ERROR);
        }

        // 生成本表关联字段的所有值
        $key = $this->data[$field];

        // 生成关联表查询条件
        $where[] = [
            $linkField => $key
        ];

        // 关联表查询结果
        $fields[$linkField] = true;
        if (!array_key_exists('_id', $fields)) {
            $fields['_id'] = false;
        }

        // 生成查询游标
        try {
            $cursor = $mongo->getCollection()->find($where, $fields)->sort($sort);
        } catch (\MongoCursorException $e) {
            trigger_error('Mongo查询失败:' . $linkMongoName, E_USER_ERROR);
            return $this;
        }

        // 生成以关联表关联字段值为键的数组
        $linkResult = [];
        try {
            while ($cursor->hasNext()) {
                $row = $cursor->getNext();
                $linkResult[] = $row;
            }
        } catch (\MongoConnectionException $e) {
            trigger_error('Mongo服务器连接失败:' . $linkMongoName, E_USER_ERROR);
        } catch (\MongoCursorTimeoutException $e) {
            trigger_error('Mongo游标读取超时:' . $linkMongoName, E_USER_ERROR);
        }

        $this->data[$linkMongoName] = $linkResult;

        return $this;
    }

    /**
     * 使用属性的方式取得行记录中的一个字段的值
     *
     * @param string $name 字段名或别名
     * @return mixed 字段的值
     */
    public function __get(string $name)
    {
        if (array_key_exists($name, $this->data)) {
            return $this->data[$name];
        }

        // 调试模式报错,运行模式,返回一个空串
        if (isDebug()) {
            trigger_error('无法在行对象中找到指定的字段:' . $name, E_USER_ERROR);
        }
        return '';
    }

    /**
     * 检查字段是否存在
     *
     * @param string $name
     * @return bool
     */
    public function exists(string $name): bool
    {
        return array_key_exists($name, $this->data);
    }

    /**
     * 用于处理 isset
     * @param $name
     * @return bool
     */
    public function __isset(string $name): bool
    {
        return array_key_exists($name, $this->data);
    }

    /**
     * 实现聚合迭代
     * @return \ArrayIterator
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->data);
    }

    /**
     * 向行对象中加一个值
     *
     * @param string|int $key
     * @param mixed $value
     */
    public function set($key, $value): void
    {
        $this->data[$key] = $value;
    }

    /**
     * JSON序列化时,自动转换格式
     * @return string
     */
    public function jsonSerialize(): string
    {
        return json($this->toArray());
    }

    /**
     * 将一个ROW对象转换成R***对象
     * @param $recordClass string R对象类型
     * @return Record
     */
    public function toRecord(string $recordClass): Record
    {
        $record = Record::instanceRecord($recordClass, $this->data);
        $record->setLoaded();
        return $record;
    }

    /**
     * 略
     * @param mixed $offset
     * @param mixed $value
     * @return mixed
     */
    public function offsetSet($offset, $value)
    {
        return $this->data[$offset] = $value;
    }

    /**
     * 为var_dump准备的
     */
    public function __debugInfo(): array
    {
        return $this->all();
    }
}