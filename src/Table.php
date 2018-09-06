<?php
declare(strict_types=1);

namespace icePHP;
/**
 * 数据库访问层,表抽象类
 * 每个表对应一个数据库连接,以便分表,参考database.config.php
 * 使用方法:
 * 1.实例表对象: $t=new STableBase('student');
 * 2.插入数据: $newId=$table->insert(array('name'=>'mouse cat','height'=>'24cm'))
 * 3.修改数据: $table->update(array('name'=>'tom jerry','weight'=>120),array('id'=>23149))
 * 4.删除数据: $table->delete(array('sex'=>0))
 * 5.查询记录集: $result=$table->select(array('id','name','birthday'),array('sex'=>1),'birthday',array(500,20));
 * 6.查询一行数据: $row=$table->row($fields,$where,$orderby);
 * 7.查询一列数据: $col=$table->col($field,$where,$orderby,$limit);
 * 8.查询一个值: $val=$table->get($field,$where,$orderby);
 * 9.按SQL查询:        $result=$table->query($sql);
 * 10.按SQL执行:        $table->execute($sql);
 * 创建一个表对象的入口，被全局函数table所调用
 * 本类在TableCached（可缓存表）的基础上增加以下功能
 * 1.row:        将父类row查询的结果转化为SRow对象
 * 2.select:        将Select查询的结果转化为SResult对象
 * 3.query:        将Query查询的结果转化为SResult对象
 * 4.getPrimaryKey：    获取表的主键字段
 * 5.meta:                    获取某一字段的数据库定义信息
 */
class Table extends TableCached
{
    /**
     * 子类里会定义此属性
     * @var TableCached 实例句柄
     */
    protected static $handle;

    /**
     * 获取单例句柄,返回具体模型类的实例对象
     */
    public static function instance() //: TableCached
    {
        if (static::$handle) {
            return static::$handle;
        }

        $class = get_called_class();
        static::$handle = new $class();
        return static::$handle;
    }

    /**
     * 取一条记录,将父类查询结果实例为SRow的对象
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @return Row 行对象/False
     * @throws \Exception
     */
    public function row($fields = null, $where = null, $orderBy = null): Row
    {
        // 查询数据
        $row = parent::row($fields, $where, $orderBy);

        // 如果没有查到
        if (!$row) {
            $row = [];
        }
        // 构造 为行对象
        return new Row($this->tableName, $row);
    }

    /**
     * 查询,返回二维数组，将父类查询结果实例为SResult对象
     * @param $field mixed
     * @param $where mixed
     * @param $order mixed
     * @param $limit mixed
     * @return Result 结果集对象
     * @throws \Exception
     */
    public function select($field = null, $where = null, $order = null, $limit = null)
    {
        //如果未提供条件/排序/分页参数
        if (!$where and !$order and !$limit) {
            //并且未提供字段参数,则表明是之前已经提供,现在要真正执行
            if (!$field) {
                return $this->go();
            }

            //只提供了字段, 并不真正执行, 这里很诡异
            return $this->fields($field);
        }

        //提供了多项,则,设置并执行
        $result = parent::selectArray($field, $where, $order, $limit);
        return new Result($this->tableName, $result);
    }

    /**
     * 不设置条件,返回指定 字段的数据
     * @param $field mixed
     * @return Result
     * @throws \Exception
     */
    public function selectAll($field): Result
    {
        return $this->fields($field)->go();
    }

    /**
     * 在设置完查询前提之后的执行请求
     * @throws \Exception
     */
    public function go(): Result
    {
        $result = parent::selectArray();
        return new Result($this->tableName, $result);
    }

    /**
     * 以SQL语句发起查询操作，将父类查询结果实例为SResult对象
     * @param string $sql
     * @param array|string $bind 要绑定的参数
     * @return Result
     * @throws \Exception
     */
    public function query(string $sql, $bind = []): Result
    {
        $result = parent::query($sql, $bind);
        return new Result($this->tableName, $result);
    }

    /**
     * 原始查询,不构造Result对象
     * @param $sql string
     * @param array|string $bind 要绑定的参数
     * @return mixed
     * @throws \Exception
     */
    public function queryRaw(string $sql, $bind = [])
    {
        return parent::query($sql, $bind);
    }

    /**
     * 获取表的主键(字段名)
     * @return string/False
     * @throws \Exception
     */
    public function getPrimaryKey()
    {
        // 取表结构
        $meta = $this->meta();

        // 逐个寻找主键
        foreach ($meta as $field) {
            if ($field['primaryKey']) {
                return $field['name'];
            }
        }
        return false;
    }

    /**
     * 获取表结构
     * @param string $name 表名
     * @return array
     * @throws \Exception
     */
    public function meta(string $name = ''): array
    {
        // 取得查询结果
        $meta = $this->getMeta($name);

        // 对结果进行修正
        $ret = [];
        foreach ($meta as $field) {
            $name = $field['COLUMN_NAME'];
            $ret[$name] = $this->metaColumns($field);
        }
        return $ret;
    }

    /**
     * 获取表的索引信息
     * @param string $name 表名
     * @return array 索引数组
     * @throws \Exception
     */
    public function index(string $name = ''): array
    {
        //获取原始索引信息
        $indexes = $this->getIndex($name);
        $ret = [];

        //逐个处理
        foreach ($indexes as $index) {
            //索引名称
            $name = $index['Key_name'];

            //列名
            $col = $index['Column_name'];

            //列信息
            $columnInfo = [
                'name' => $col,
                'null' => $index['Null'] ? true : false,//列中是否含有NULL
                'subPart' => $index['Sub_part'],//如果列只是被部分地编入索引，则为被编入索引的字符的数目。如果整列被编入索引，则为NULL
            ];

            //索引中的第一列,则创建
            if ($index['Seq_in_index'] == 1) {
                $ret[$name] = [
                    'tableName' => $index['Table'], //所属表名
                    'isUnique' => !$index['Non_unique'], //是否唯一索引
                    'collation' => $index['Collation'], //这个有A和NULL, A是不区分大小写?
                    'columns' => [$col => $columnInfo],//本索引中包含的列
                    'cardinality' => $index['Cardinality'], //索引中唯一值的数目的估计值。
                    'packed' => $index['Packed'],//指示关键字如何被压缩。如果没有被压缩，则为NULL。
                    'type' => $index['Index_type'],//用过的索引方法（BTREE, FULLTEXT, HASH, RTREE）。
                    'comment' => $index['Comment'],//说明
                ];
            } else {
                //索引中的后继列,则只增加列信息
                $ret[$name]['columns'][$col] = $columnInfo;
            }
        }

        return $ret;
    }

    /**
     * 处理单个字段的元类型
     * @param array $row
     * @return array
     */
    private function metaColumns($row)
    {
        // 字段的各个属性
        $field = [];

        // 字段名
        $field['name'] = $row['COLUMN_NAME'];

        // 字段精度默认值
        $field['scale'] = null;
        $queryArray = false;

        // 字段类型
        $type = $row['COLUMN_TYPE'];
        if (preg_match('/^(.+)\((\d+),(\d+)/', $type, $queryArray)) {
            // abc(123,234)
            $field['type'] = $queryArray[1];
            $field['maxLength'] = is_numeric($queryArray[2]) ? $queryArray[2] : -1;
            $field['scale'] = is_numeric($queryArray[3]) ? $queryArray[3] : -1;
        } elseif (preg_match('/^(.+)\((\d+)/', $type, $queryArray)) {
            // abc(123)
            $field['type'] = $queryArray[1];
            $field['maxLength'] = is_numeric($queryArray[2]) ? $queryArray[2] : -1;
        } elseif (preg_match('/^(enum)\((.*)\)$/i', $type, $queryArray)) {
            // enum(item[,item]...)
            $field['type'] = $queryArray[1];
            $arr = explode(",", $queryArray[2]);
            $field['enums'] = [];
            foreach ($arr as $k => $item) {
                $field['enums'][$k] = trim($item, '\'\"');
            }
            $zlen = max(array_map("strlen", $arr)) - 2; // PHP >= 4.0.6
            $field['maxLength'] = ($zlen > 0) ? $zlen : 1;
        } elseif (preg_match('/^(set)\((.*)\)$/i', $type, $queryArray)) {
            // set(item[,item]...)
            $field['type'] = $queryArray[1];
            $arr = explode(",", $queryArray[2]);
            $field['sets'] = [];
            foreach ($arr as $k => $item) {
                $field['sets'][$k] = trim($item, '\'\"');
            }
            $zlen = max(array_map("strlen", $arr)) - 2; // PHP >= 4.0.6
            $field['maxLength'] = ($zlen > 0) ? $zlen : 1;
        } else {
            $field['type'] = $type;
            $field['maxLength'] = -1;
        }

        // 是否不允许空
        $field['notNull'] = ($row['IS_NULLABLE'] != 'YES');

        // 是否主键
        $field['primaryKey'] = ($row['COLUMN_KEY'] == 'PRI');

        // 是否自增长
        $field['autoIncrement'] = (strpos($row['EXTRA'], 'auto_increment') !== false);

        // 是否二进制
        $field['binary'] = (strpos($type, 'blob') !== false);

        // 是否无符
        $field['unsigned'] = (strpos($type, 'unsigned') !== false);

        // 看看有没有默认值
        if (!$field['binary']) {
            $d = $row['COLUMN_DEFAULT'];
            if ($d != '' && $d != 'NULL') {
                $field['hasDefault'] = true;
                $field['defaultValue'] = $this->convertType($d, $field['type']);
            } else {
                $field['hasDefault'] = false;
            }
        }

        // 字段备注
        $field['description'] = isset($row['COLUMN_COMMENT']) ? str_replace(["\r", "\n"], '', $row['COLUMN_COMMENT']) : '';
        if (!$field['description'] and $field['name'] == 'created') {
            $field['description'] = '创建时间';
        }
        if (!$field['description'] and $field['name'] == 'updated') {
            $field['description'] = '修改时间';
        }

        return $field;
    }

    /**
     * 强制转换数据类型,为上一方法使用
     * @param mixed $value
     * @param string $type
     * @return mixed
     */
    private function convertType($value, $type)
    {
        $type = strtolower($type);

        // 整型转换
        if (in_array($type, ['bigint', 'int', 'mediumint', 'smallint', 'tinyint', 'year'])) {
            return (int)$value;
        }

        // 浮点转换
        if (in_array($type, ['float', 'double', 'decimal'])) {
            return (float)$value;
        }

        // 布尔转换
        if (in_array($type, ['bit'])) {
            return (bool)$value;
        }

        // 其它只能是字符串了
        return $value;
    }
}