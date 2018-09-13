<?php
declare(strict_types=1);

namespace icePHP;
/**
 * Select返回的结果集对象
 * @author Ice
 * 结果集对象还有许多方法:
 **** all 返回结果集数据(二维数组)
 **** count 返回结果集行数
 **** row() 返回一个对象(对应一条记录),再次调用时自动返回下一个
 **** row($n) 返回第几个对象
 **** reset()    重置游标
 **** map  一对一映射 关联另一个表,返回仍旧是结果集对象,但此对象的数据已经扩展
 **** join 一对多映射,关联另一个表,返回仍旧是本结果集,但每行(对象)增加了一个属性
 **** 此属性是另一个表的SResult对象
 * */
class Result implements \IteratorAggregate, \ArrayAccess, \JsonSerializable
{
    use LinkTrait;

    // 行数
    private $count;

    // 游标
    private $cursor = 0;

    // 行对象数组
    protected $rows = [];

    /**
     * 给定表名和多行数组,构造一个结果集对象
     *
     * @param string $tableName
     * @param array $data
     * @throws MysqlException|TableException
     */
    public function __construct(string $tableName, array $data)
    {
        self::construct($tableName, $data);

        // 计数,创建行对象
        $this->data = $data;
        $this->row2Obj();
    }

    /**
     * 行数据转换成行对象
     * @throws TableException|MysqlException
     */
    private function row2Obj(): void
    {
        // 必须有数据,且是数组
        if ($this->data and is_array($this->data)) {
            // 保存计数
            $this->count = count($this->data);

            // 逐行转换为行对象
            $this->rows = [];
            foreach ($this->data as $r) {
                $this->rows[] = new Row($this->table, $r);
            }
        }
    }

    /**
     * 重置游标
     */
    public function reset(): Result
    {
        $this->cursor = 0;
        return $this;
    }

    /**
     * 获取结果集的行数
     *
     * @return int
     */
    public function count(): int
    {
        return intval($this->count);
    }

    /**
     * 判断结果集是否为空
     * @return bool
     */
    public function isEmpty(): bool
    {
        return !$this->count();
    }

    /**
     * 生成本表 需要关联字段的所有值,为下面两个MAP方法使用
     *
     * @param string $field 字段
     * @return array|null
     */
    private function getMapKeys(string $field): ?array
    {
        $keys = [];
        foreach ($this->data as $row) {
            if (array_key_exists($field, $row) and !in_array($row[$field], $keys) and !is_null($row[$field])) {
                $keys[] = $this->table->escape($row[$field]);
            }
        }

        // 如果本表关联字段无值,不处理
        if (!count($keys)) {
            return null;
        }

        return $keys;
    }

    /**
     * 如果需要,将关联的数据填充到本表字段中,为下面两个MAP方法使用
     *
     * @param array $linkResult
     * @param string $field
     * @param array $row
     * @param string|int $key
     * @return boolean
     */
    private function mapFill(array $linkResult, string $field, array $row, $key): bool
    {
        // 如果没有对应的关联,则不扩展
        if (array_key_exists($row[$field], $linkResult)) {
            // 关联的记录
            $linkRow = $linkResult[$row[$field]];

            // 处理每一个关联的字段
            foreach ($linkRow as $k => $v) {
                // 如果本表已经有此字段,则不扩展
                if (!array_key_exists($k, $row)) {
                    $row[$k] = $v;
                }
            }
            $this->data[$key] = $row;
            return true;
        }
        return false;
    }

    /**
     * 一对一映射
     *
     * @param string|Table $linkTableName
     * @param mixed $relation 参考 _relation 方法
     * @param mixed $fields
     * @param mixed $where
     * @return Result
     * @throws MysqlException|TableException
     */
    public function map($linkTableName, $relation, $fields = '*', $where = []): Result
    {
        // 生成关联表模型对象
        $table = $this->makeTable($linkTableName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation);

        // 生成本表关联字段的所有值
        $keys = $this->getMapKeys($field);
        if (!$keys) {
            return $this;
        }

        // 生成关联表查询条件
        // $where []= $this->table->markField ( $linkField ) . ' in ("' . join ( '","', $keys ) . '") ';
        $where[$linkField . ' in'] = $keys;

        // 关联表查询结果
        $linkData = $table->select($this->append($fields, $linkField), $where)
            ->toArray();
        // 生成以关联表关联字段值为键的数组
        $linkResult = [];
        foreach ($linkData as $row) {
            $linkResult[$row[$linkField]] = $row;
        }

        $fields = $this->table->createFields($fields);

        // 为本表每行数据补充字段
        foreach ($this->data as $key => $row) {
            // 字段补充成功
            if ($this->mapFill($linkResult, $field, $row, $key)) {
                continue;
            }

            // 没有对应成功,补充空字符串
            $this->data[$key] = $this->appendNull($row, $fields);
        }

        $this->row2Obj();
        return $this;
    }

    /**
     * 连接到Mongo的Collection上
     *
     * @param string $linkMongoName Mongo的集合名称
     * @param mixed $relation 对应关系(本表字段=>要连接的表的字段)
     * @param array $fields 目标表的字段,取出,使用
     * @return Result
     * @throws MongoException|TableException|MysqlException|\MongoConnectionException|\MongoCursorTimeoutException
     */
    public function mapMongo(string $linkMongoName, $relation, array $fields = []): Result
    {
        if (empty($fields)) {
            return $this;
        }

        // 生成关联表模型对象
        $mongo = mongo($linkMongoName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation, 'mongo');

        // 生成本表关联字段的所有值
        $keys = $this->getMapKeys($field);
        if (!$keys) {
            return $this;
        }

        // 生成关联表查询条件
        $where = [
            $linkField => [
                '$in' => $keys
            ]
        ];

        // 关联表查询结果
        $fields[$linkField] = true;
        if (!array_key_exists('_id', $fields)) {
            $fields['_id'] = false;
        }

        // 生成查询游标
        $cursor = $mongo->getCollection()->find($where, $fields);

        // 生成以关联表关联字段值为键的数组
        $linkResult = [];
        while ($cursor->hasNext()) {
            $row = $cursor->getNext();
            $linkResult[$row[$linkField]] = $row;
        }

        // 为本表每行数据补充字段
        foreach ($this->data as $key => $row) {
            if ($this->mapFill($linkResult, $field, $row, $key)) {
                continue;
            }

            foreach ($fields as $k => $v) {
                // 如果本表已经有此字段,则不扩展
                if ($v and !array_key_exists($k, $row)) {
                    // 扩展值为空,因为没有对应
                    $row[$k] = null;
                }
            }

            $this->data[$key] = $row;
        }

        $this->row2Obj();
        return $this;
    }

    /**
     * 一对多映射
     *
     * @param string|Table $linkTableName
     * @param mixed $relation 参考 _relation 方法
     * @param mixed $fields
     * @param mixed $where
     * @param mixed $orderBy
     * @return Result
     * @throws TableException|MysqlException
     */
    public function join($linkTableName, $relation, $fields = '*', $where = [], $orderBy = ''): Result
    {
        // 生成关联表模型对象
        $table = $this->makeTable($linkTableName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation);

        // 生成本表关联字段的所有值
        $keys = $this->getMapKeys($field);
        if (!$keys) {
            return $this;
        }

        // 生成关联表查询条件
        // $where [] = $table->markField ( $linkField ) . ' in ("' . join ( '","', $keys ) . '") ';
        $where[$linkField . ' in'] = $keys;

        // 关联表查询结果
        $linkData = $table->select($this->append($fields, $linkField), $where, $orderBy)
            ->all();

        // 生成以关联表关联字段值为键的数组
        $linkResult = [];
        foreach ($linkData as $row) {
            $key = $row[$linkField];
            if (!array_key_exists($key, $linkResult)) {
                $linkResult[$key] = [];
            }
            $linkResult[$key][] = $row;
        }

        // 转换每个值对应的结果为结果集对象
        foreach ($linkResult as $k => $v) {
            $linkResult[$k] = new Result($linkTableName, $v);
        }

        // 为本表每行数据补充字段
        foreach ($this->data as $key => $row) {

            // 如果没有对应的关联,则不扩展
            if (array_key_exists($row[$field], $linkResult)) {
                // 关联的记录
                $linkObj = $linkResult[$row[$field]];

                // 如果本表已经有此字段,则不扩展
                if (!array_key_exists($table->name(), $row)) {
                    $row[$table->alias()] = $linkObj;
                }

                $this->data[$key] = $row;
            }
        }

        $this->row2Obj();
        return $this;
    }

    /**
     * 一对多关联到Mongo表上
     *
     * @param string $linkMongoName Mongo表名
     * @param mixed $relation
     * @param array $fields
     * @param mixed $where
     * @param mixed $sort
     * @return Result
     * @throws MongoException|TableException|\MongoCursorException|\MongoCursorTimeoutException|\MongoConnectionException|MysqlException
     */
    public function joinMongo(string $linkMongoName, $relation, array $fields = [], $where = [], $sort = []): Result
    {
        if (empty($fields)) {
            return $this;
        }

        // 生成关联表模型对象
        $mongo = mongo($linkMongoName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation, 'mongo');

        // 生成本表关联字段的所有值
        $keys = $this->getMapKeys($field);
        if (!$keys) {
            return $this;
        }

        $where[] = [
            $linkField => [
                '$in' => $keys
            ]
        ];

        // 关联表查询结果
        $fields[$linkField] = true;
        if (!array_key_exists('_id', $fields)) {
            $fields['_id'] = false;
        }

        // 生成查询游标
        $cursor = $mongo->getCollection()->find($where, $fields)->sort($sort);

        // 生成以关联表关联字段值为键的数组
        $linkResult = [];
        while ($cursor->hasNext()) {
            $row = $cursor->getNext();
            $key = $row[$linkField];
            if (!array_key_exists($key, $linkResult)) {
                $linkResult[$key] = [];
            }
            $linkResult[$key][] = $row;
        }

        // 转换每个值对应的结果为结果集对象
        foreach ($linkResult as $k => $v) {
            $linkResult[$k] = new Result($linkMongoName, $v);
        }

        // 为本表每行数据补充字段
        foreach ($this->data as $key => $row) {

            // 如果没有对应的关联,则不扩展
            if (array_key_exists($row[$field], $linkResult)) {
                // 关联的记录
                $linkObj = $linkResult[$row[$field]];

                // 如果本表已经有此字段,则不扩展
                if (!array_key_exists($linkMongoName, $row)) {
                    $row[$linkMongoName] = $linkObj;
                }

                $this->data[$key] = $row;
            }
        }

        $this->row2Obj();
        return $this;
    }

    /**
     * 略
     * @param mixed $offset
     * @return mixed
     */
    public function offsetGet($offset)
    {
        return $this->rows[$offset];
    }

    /**
     * 略
     * @param mixed $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value): void
    {
        //修改DATA
        if ($value instanceof Row) {
            $this->data[$offset] = $value->all();
        } else {
            $this->data[$offset] = $value;
        }

        //修改ROWS
        $this->rows[$offset] = $value;
    }

    /**
     * 实现聚合迭代
     * @return \ArrayIterator
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->rows);
    }

    /**
     * 实现JSON序列化接口的方法
     * @return array
     */
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    /**
     * 将结果集对象转换成记录对象数组
     * @param $recordClass string 具体Record对象类名
     * @return array[SRecord]
     */
    public function toRecords(string $recordClass): array
    {
        $array = [];
        foreach ($this->rows as $row) {
            /**
             * @var $row Row
             */
            $record = Record::instanceRecord($recordClass, $row->toArray());
            $record->setLoaded();
            $array[] = $record;
        }
        return $array;
    }

    /**
     * 一对多统计  副表进行分组并统计结果,结果与主表一对表
     * @deprecated 未被使用
     * @param string|Table $linkTableName
     * @param mixed $relation 参考 _relation 方法
     * @param mixed $fields
     * @param mixed $where
     * @param mixed $orderBy
     * @return Result
     * @throws TableException|MysqlException
     */
    private function mapGroup($linkTableName, $relation, $fields = '*', $where = [], $orderBy = ''): Result
    {
        // 生成关联表模型对象
        $table = $this->makeTable($linkTableName);

        // 生成本表关联字段与关联表关联字段
        list ($field, $linkField) = $this->relation($relation);

        // 生成本表关联字段的所有值
        $keys = $this->getMapKeys($field);
        if (!$keys) {
            return $this;
        }

        $linkResult = [];
        foreach ($keys as $value) {
            $where[$linkField] = $value;
            $linkData = $table->groupBy($linkField)->select($this->append($fields, $linkField), $where, $orderBy)
                ->toArray();

            //如果没有关联数据
            if (!$linkData) {
                continue;
            }

            $key = $linkData[0][$linkField];
            if (!array_key_exists($key, $linkResult)) {
                $linkResult[$key] = [];
            }
            $linkResult[$key] = $linkData[0];
        }

        $fields = $this->table->createFields($fields);

        // 为本表每行数据补充字段
        foreach ($this->data as $key => $row) {
            // 字段补充成功
            if ($this->mapFill($linkResult, $field, $row, $key)) {
                continue;
            }

            // 没有对应成功,补充空字符串
            $this->data[$key] = $this->appendNull($row, $fields);
        }
        $this->row2Obj();
        return $this;
    }

    /**
     * 为var_dump准备的
     */
    public function __debugInfo(): array
    {
        return $this->all();
    }
}