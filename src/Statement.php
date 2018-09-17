<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 构造PDOStatement语句的类
 * 根据各种情况的输入参数,构造用于Prepare的语句及参数表,构造完整SQL用于日志及调试
 * @author 蓝冰
 */
class Statement
{
    // 基表名
    private $table;

    /**
     * 使用的数据库引擎,通常是Mysql
     *
     * @var Mysql
     */
    private $db;

    // 操作:insert/insertIgnore/replace/crease/delete/deleteAll/execute/exist/query/select/selectHandle/update
    private $operation;

    // 保存查询操作前参数
    public $distinct = false, $fields, $where, $orderBy, $limit, $groupBy, $having;

    // 保存修改操作前参数
    private $row;

    // 构造出来的完整SQL
    private $sql;

    // 构造出来的用于Prepare的SQL
    private $prepare;

    // 构造出来的用于执行的参数表
    private $params = [];

    // 本语句涉及的表名列表
    private $tables = [];

    //将要原生连接的表以及连接条件
    private $joinOperations = [], $joins = [], $ons = [];

    /**
     * 构造 方法, 创建针对指定表的 语句对象
     * @param string $table 基表名
     */
    public function __construct(string $table)
    {
        $this->table = $table;
        $this->db = Mysql::instance();
    }

    /**
     * 判断是否是空对象,只要指明了操作,就不算空了
     * @return boolean
     */
    public function isNull(): bool
    {
        return !$this->operation;
    }

    /**
     * 判别是否是查询操作,此类操作后继要Fetch,否则不要Fetch(会报错)
     * @return boolean
     */
    public function isQuery(): bool
    {
        return in_array($this->operation, ['query', 'select', 'selectHandle']);
    }

    /**
     * 返回本次语句中的表名数组
     * @return array
     */
    public function getTable(): array
    {
        return $this->tables;
    }

    /**
     * 返回本次语句的Prepare语句
     */
    public function getPrepare(): string
    {
        return $this->prepare;
    }

    /**
     * 返回本次语句的操作
     */
    public function getOperation(): string
    {
        return $this->operation;
    }

    /**
     * 返回本次语句的完整SQL
     */
    public function getSql(): string
    {
        return $this->sql;
    }

    /**
     * 返回本次语句的参数数组
     */
    public function getParams(): array
    {
        return $this->params;
    }

    /**
     * 记录exist操作及相关参数
     * @param mixed $where 只需要条件
     * @return Statement
     */
    public function exist($where = null): Statement
    {
        $this->operation = 'exist';
        $this->fields = '*';
        if ($where) {
            $this->where = $where;
        }
        return $this;
    }

    /**
     * 记录join操作及相关参数
     * @param $operation string left/right/inner/outer
     * @param $table mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function join(string $operation, $table, $on = null): Statement
    {
        if (count($this->joins) > count($this->ons)) {
            trigger_error('关联时,关联关系不足', E_USER_ERROR);
        }
        $this->joinOperations[] = $operation;
        $this->joins[] = $table;
        if ($on) {
            $this->on($on);
        }
        return $this;
    }

    /**
     * 记录on操作参数
     * @param $relation mixed 关联条件
     * @return $this
     */
    public function on($relation): Statement
    {
        if (count($this->ons) >= count($this->joins)) {
            trigger_error('关联时,关联关系过多', E_USER_ERROR);
        }
        $this->ons[] = $relation;
        return $this;
    }

    /**
     * 记录query操作及相关参数
     *
     * @param string $sql 一条已经写好的SQL
     * @param array|string $bind 要绑定的参数
     * @return Statement
     */
    public function query(string $sql, $bind = []): Statement
    {
        $this->operation = 'query';
        $this->params = $bind;
        $this->sql = $sql;
        return $this;
    }

    /**
     * 记录execute操作及相关参数
     * @param string $sql 一条已经写好的SQL
     * @param array|string $bind 要绑定的参数
     * @return Statement
     */
    public function execute(string $sql, $bind = []): Statement
    {
        $this->operation = 'execute';
        $this->sql = $sql;
        $this->params = $bind;
        return $this;
    }

    /**
     * 为Select和SelectHandle设置查询前参数
     * 被select,selectHandle调用
     *
     * @param  $fields mixed 字段列表
     * @param  $where  mixed  查询条件
     * @param  $orderBy mixed 排序依据
     * @param  $limit string|array 分页
     * @return Statement
     */
    private function setSelectArgument($fields, $where, $orderBy, $limit): Statement
    {
        if ($fields) {
            $this->fields = $fields;
        }
        if (!is_null($where)) {
            $this->where = $where;
        }
        if ($orderBy) {
            $this->orderBy = $orderBy;
        }
        if ($limit) {
            $this->limit = $limit;
        }
        return $this;
    }

    /**
     * 记录select操作及相关参数
     *
     * @param mixed $fields 字段列表
     * @param mixed $where 条件
     * @param mixed $orderBy 排序
     * @param mixed $limit 分页
     * @return Statement
     */
    public function select($fields = null, $where = null, $orderBy = null, $limit = null): Statement
    {
        $this->operation = 'select';
        return $this->setSelectArgument($fields, $where, $orderBy, $limit);
    }

    /**
     * 记录selectHandle操作及相关参数,
     *
     * @param mixed $fields 字段列表
     * @param mixed $where 条件
     * @param mixed $orderBy 排序
     * @param mixed $limit 分页
     * @return Statement
     */
    public function selectHandle($fields = null, $where = null, $orderBy = null, $limit = null): Statement
    {
        $this->operation = 'selectHandle';
        return $this->setSelectArgument($fields, $where, $orderBy, $limit);
    }

    /**
     * 记录insert操作及相关参数
     *
     * @param array $row 要插入的数据
     * @return Statement
     */
    public function insert(array $row): Statement
    {
        $this->operation = 'insert';
        $this->row = $row;
        return $this;
    }

    /**
     * 记录Inserts操作及相关参数
     * @param array $rows 要插入的多行数据
     * @return $this
     */
    public function inserts(array $rows): Statement
    {
        $this->operation = 'inserts';
        $this->row = $rows;
        return $this;
    }

    /**
     * 记录操作及相关参数,此方法的功能请参考Table类
     *
     * @param array $row
     * @return Statement
     */
    public function insertIgnore(array $row): Statement
    {
        $this->operation = 'insertIgnore';
        $this->row = $row;
        return $this;
    }

    /**
     * 记录操作及相关参数,此方法的功能请参考Table类
     *
     * @param array $row
     * @return Statement
     */
    public function replace(array $row): Statement
    {
        $this->operation = 'replace';
        $this->row = $row;
        return $this;
    }

    /**
     * 记录update操作及相关参数
     *
     * @param array $row 要修改的数据
     * @param mixed $where 查询条件
     * @return Statement
     */
    public function update(array $row, $where): Statement
    {
        $this->operation = 'update';
        if ($where) {
            $this->where = $where;
        }
        $this->row = $row;
        return $this;
    }

    // increase/decrease方法中的操作符及增量
    private $creaseOperator, $creaseDiff;

    /**
     * 记录操作及相关参数,此方法的功能请参考Table类
     *
     * @param string $operator
     * @param mixed $fields
     * @param mixed $where
     * @param float $diff
     * @return Statement
     */
    public function crease(string $operator = '+', $fields, $where, float $diff): Statement
    {
        $this->operation = 'crease';
        if ($fields) {
            $this->fields = $fields;
        }
        if ($where) {
            $this->where = $where;
        }
        $this->creaseOperator = $operator;
        $this->creaseDiff = $diff;
        return $this;
    }

    /**
     * 记录delete操作及相关参数
     *
     * @param mixed $where 条件
     * @return Statement
     */
    public function delete($where): Statement
    {
        $this->operation = 'delete';
        if ($where) {
            $this->where = $where;
        }
        return $this;
    }

    /**
     * 记录全部删除操作
     *
     * @return Statement
     */
    public function deleteAll(): Statement
    {
        $this->operation = 'deleteAll';
        return $this;
    }

    /**
     * 输入已经完成,开始创建语句信息
     * 生成prepare语句
     * 生成sql语句
     * 生成params参数数组
     * 生成表个数/表名
     *
     * @return string
     * @throws MysqlException
     */
    public function create(): string
    {
        switch ($this->operation) {
            case 'exist':
                // 判断是否存在满足条件的记录
                return $this->createExist();
            case 'query':
            case 'repair':
            case 'optimize':
            case 'call':
                // 直接SQL查询
                return $this->createQuery();
            case 'execute':
                // 直接SQL操作
                return $this->createExecute();
            case 'insert':
                // 插入
            case 'replace':
                // 存在则替换,否则插入
            case 'insertIgnore':
                // 存在则忽略,否则插入
                return $this->createAdd();
            case 'inserts':
                //多行插入
                return $this->createInserts();
            case 'update':
                // 修改
                return $this->createUpdate();
            case 'delete':
                // 删除
                return $this->createDelete();
            case 'deleteAll':
                // 删除全部
                return $this->createDeleteAll();
            case 'select':
                // 查询
            case 'selectHandle':
                // 查询并返回句柄
                return $this->createSelect();
            case 'crease':
                // 增减
                return $this->createCrease();
        }
        trigger_error('无法识别或不支持的SQL命令:' . $this->operation, E_USER_ERROR);
        return '';
    }

    /**
     * 构造与值列表个数相同的占位符列表
     *
     * @param array $values
     * @return array
     */
    private function createHolder(array $values): array
    {
        return array_fill(0, count($values), '?');
    }

    /**
     * 具体生成exist的相关数据,利用了select
     * @return string
     */
    private function createExist(): string
    {
        // 先构造子查询
        $this->createSelect();

        // 使用子查询构造存在查询
        $this->prepare = $this->db->createExist($this->prepare);
        $this->sql = $this->db->createExist($this->sql);

        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体生成query的相关数据
     * @return string
     */
    private function createQuery(): string
    {
        // 已经直接传递了SQL,Prepare与SQL相同
        $this->prepare = $this->sql;

        // 查找其中的表名
        $this->tables = $this->db->getNameFromQuery($this->sql);

        return $this->prepare;
    }

    /**
     * 具体生成execute的相关数据
     * @return string
     */
    private function createExecute(): string
    {
        // 已经传递了SQL,Prepare语句与SQL相同
        $this->prepare = $this->sql;

        // 查找SQL中涉及的表
        $this->tables = $this->db->getNameFromExecute($this->sql);

        return $this->sql;
    }

    /**
     * 具体生成Select的相关数据
     * @return string
     */
    private function createSelect(): string
    {
        // 构造 字段列表
        $fields = $this->db->createFields($this->fields);
        if ($fields === false) {
            return '';
        }

        // 构造Where相关三参数
        list ($whereSql, $wherePrepared, $whereParams) = $this->db->createWhere($this->where);

        // 构造Having相关三参数
        list ($havingSql, $havingPrepared, $havingParams) = $this->db->createHaving($this->having);

        //构造关联部件
        $joinSql = ' ' . $this->db->createJoins($this->joinOperations, $this->joins, $this->ons);

        // 其它参数不需要区别处理
        $groupBy = $this->db->createGroupBy($this->groupBy);
        $orderBy = $this->db->createOrderBy($this->orderBy);
        $limit = $this->db->createLimit($this->limit);

        // 分别处理Distinct和Select
        if ($this->distinct) {
            // Distinct 生成SQL和Prepare
            $this->sql = $this->db->createDistinct($this->table . $joinSql, $fields, $whereSql, $orderBy, $groupBy, $havingSql, $limit);
            $this->prepare = $this->db->createDistinct($this->table . $joinSql, $fields, $wherePrepared, $orderBy, $groupBy, $havingPrepared, $limit);
        } else {
            // Select 生成SQL和Prepare
            $this->sql = $this->db->createSelect($this->table . $joinSql, $fields, $whereSql, $orderBy, $groupBy, $havingSql, $limit);
            $this->prepare = $this->db->createSelect($this->table . $joinSql, $fields, $wherePrepared, $orderBy, $groupBy, $havingPrepared, $limit);
        }

        $this->params = array_merge($whereParams, $havingParams);
        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体生成insert/insertIgnore/replace的相关数据
     * 此三种方法类似
     * @return string
     * @throws MysqlException
     */
    private function createAdd(): string
    {
        // 构造名值对
        list ($fields, $values) = $this->db->createRow($this->row);

        // 所有的值都是参数
        $this->params = $values;

        // 生成构造到SQL中的值数组和Prepare中的值数组(定位符)
        $vSql = $this->db->markValueArray($values);
        $vHolder = $this->createHolder($values);

        // 三种插入,只有SQL语句不同
        switch ($this->operation) {
            case 'insert':
                $this->sql = $this->db->createInsert($this->table, $fields, $vSql);
                $this->prepare = $this->db->createInsert($this->table, $fields, $vHolder);
                break;
            case 'insertIgnore':
                $this->sql = $this->db->createInsertIgnore($this->table, $fields, $vSql);
                $this->prepare = $this->db->createInsertIgnore($this->table, $fields, $vHolder);
                break;
            case 'replace':
                $this->sql = $this->db->createReplace($this->table, $fields, $vSql);
                $this->prepare = $this->db->createReplace($this->table, $fields, $vHolder);
                break;
            default:
                trigger_error('不识别的命令:' . $this->operation, E_USER_ERROR);
        }

        $this->params = $values;
        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体生成Inserts的相关数据
     * @return string
     * @throws MysqlException
     */
    private function createInserts(): string
    {
        //以第一行数据的标题为字段名基准
        list($fields,) = $this->db->createRow($this->row[0]);

        //逐行处理
        $data = $holders = $params = [];
        foreach ($this->row as $row) {
            //本行数据加MYSQL定界符
            list($rowFields, $row) = $this->db->createRow($row);

            //每个字段必须有,没有的为空字符串
            $r = [];
            foreach ($fields as $f) {
                $params[] = $r[] = in_array($f, $rowFields) ? $row[array_search($f, $rowFields)] : '';
            }

            //行数据集
            $data[] = $this->db->markValueArray($r);

            //占位符集合
            $holders[] = $this->createHolder($r);
        }

        $this->sql = $this->db->createInserts($this->table, $fields, $data);
        $this->prepare = $this->db->createInserts($this->table, $fields, $holders);
        $this->params = $params;
        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 根据字段列表,增减操作符,增量,构造 字段SQL,Prepare,Params列表
     *
     * @param array $fields
     * @param string $op
     * @param float $diff
     * @return array (array(sql),array(prepare),array(params))
     */
    private function createCreaseFields(array $fields, string $op, float $diff): array
    {
        $diff = floatval($diff);

        $sqlArray = $prepares = $params = [];

        foreach ($fields as $field) {
            // 对字段名进行规范化
            $f = $this->db->markField($field);

            // 用于SQL中
            $sqlArray[] = $this->db->createCrease($f, $op, $diff);

            // 用于Prepare中
            $prepares[] = $this->db->createCrease($f, $op, '?');

            // 用于Params中
            $params[] = $diff;
        }

        return [$sqlArray, $prepares, $params];
    }

    /**
     * 具体生成crease相关数据
     * @return string
     */
    private function createCrease(): string
    {
        // 构造 字段
        $fields = $this->db->createFields($this->fields);
        if (!$fields) {
            return '';
        }

        // 构造查询条件
        list ($where, $wherePrepare, $whereParams) = $this->db->createWhere($this->where);
        if ($where === false) {
            return '';
        }

        // 构造Crease的字段
        $fields = explode(',', $fields);
        list ($sets, $setsPrepare, $setsParams) = $this->createCreaseFields($fields, $this->creaseOperator, $this->creaseDiff);

        $this->sql = $this->db->createUpdate($this->table, $sets, $where);
        $this->prepare = $this->db->createUpdate($this->table, $setsPrepare, $wherePrepare);
        $this->params = array_merge($setsParams, $whereParams);

        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体生成Update的相关数据
     * @return string
     * @throws MysqlException
     */
    private function createUpdate(): string
    {
        // 处理字段名值数组
        list ($fields, $values) = $this->db->createRow($this->row);

        // 处理条件三项
        list ($whereSql, $wherePrepare, $whereParams) = $this->db->createWhere($this->where);
        if ($whereSql === false) {
            return '';
        }

        // 更新时未指定条件
        if (!$whereSql) {
            trigger_error('Update操作必须指定查询条件,不允许全表修改', E_USER_ERROR);
        }

        // 字段数组中的三项
        $set = $setPrepare = [];
        foreach ($fields as $k => $field) {
            $set[] = $this->db->createSet($field, $values[$k]);
            $setPrepare[] = $this->db->createSet($field, '?');
        }

        $this->sql = $this->db->createUpdate($this->table, $set, $whereSql);
        $this->prepare = $this->db->createUpdate($this->table, $setPrepare, $wherePrepare);
        $this->params = array_merge($values, $whereParams);

        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体处理DeleteAll的相关数据
     */
    private function createDeleteAll(): string
    {
        // 生成三项
        $this->sql = $this->db->createDelete($this->table, '');
        $this->prepare = $this->sql;
        $this->params = [];

        $this->tables = [$this->table];
        return $this->prepare;
    }

    /**
     * 具体 Delete的相关数据
     *
     * @return mixed
     */
    private function createDelete()
    {
        // 条件三项
        list ($condition, $conditionPrepare, $conditionParams) = $this->db->createWhere($this->where);
        if ($condition === false) {
            return false;
        }

        // 更新时未指定条件
        if (!$condition) {
            trigger_error('Delete操作必须指定查询条件,不允许全表删除', E_USER_ERROR);
        }

        // 生成三项
        $this->sql = $this->db->createDelete($this->table, $condition);
        $this->prepare = $this->db->createDelete($this->table, $conditionPrepare);
        $this->params = $conditionParams;

        $this->tables = [$this->table];
        return $this->prepare;
    }
}