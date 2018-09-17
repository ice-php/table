<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 增加缓存功能
 * disableCache 临时阻止一次缓存
 * query/execute 为父类的查询加缓存
 *
 * 构造一些常用数据操作
 * showTables 显示所有表
 * replace 有则替换，无则插入
 * insertIgnore 有则忽略，无则插入
 * insert/delete/deleteAll/col/get/row/increase/decrease/update/count/exist/sum
 *
 * 为子类提供一些基础数据库结构操作方法
 * getMeta
 *
 * 扩展增加级联查询
 * distinct/fields/where/orderby/groupby/having/limit
 *
 * 如果配置中不允许 多表关联查询,则检查出来,并抛出异常
 */
abstract class TableCached extends TableBase
{
    //前置钩子用到的常量
    const INTERRUPT = '操作被前置钩子所阻止';

    // 包含所有钩子相关代码
    use TableHook;

    // 本表是否强制要求文件缓存
    private $fileCache = false;

    /**
     * 根据表的别名构造表对象,同时构造语句对象
     *
     * @param string $alias 表别名
     * @param $fileCache bool 是否要对本表进行文件缓存
     */
    protected function __construct(string $alias, bool $fileCache = false)
    {
        // 调用父类的构造方法
        parent::__construct($alias);

        // 设置是否文件缓存
        $this->fileCache = $fileCache;

        // 先初始化一下语句
        $this->clear();
    }

    /**
     * 静态化,工厂方法
     * 因为本类的子类要使用instance创建实例对象,但参数不同,
     * 导致本方法不能使用instance作为方法名
     *
     * @param string $alias
     * @param bool $fileCache
     * @return Table      注:就是Table,而不是TableCache
     */
    static public function getInstance(string $alias, bool $fileCache = false): Table
    {
        // 保存每个表对象
        static $tables = [];

        // 如果尚未创建,则创建并缓存
        if (!isset($tables[$alias])) {
            $tables[$alias] = new static($alias, $fileCache);
        }

        // 返回缓存后的表对象
        return $tables[$alias];
    }

    /**
     * 语句对象
     * @var Statement
     */
    private $statement;

    /**
     * 清除所有预保存参数,用于各种操作完成后
     * 实际上是重新生成一个空的语句对象
     */
    private function clear(): TableCached
    {
        $this->statement = new Statement($this->tableName);
        return $this;
    }

    /**
     * 设置表关联(inner)
     * @param $join mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function join($join, $on = null): TableCached
    {
        return $this->innerJoin($join, $on);
    }

    /**
     * 设置表关联(left)
     * @param $join mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function leftJoin($join, $on = null): TableCached
    {
        $this->statement->join('left', $join, $on);
        return $this;
    }

    /**
     * 设置表关联(right)
     * @param $join mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function rightJoin($join, $on = null): TableCached
    {
        $this->statement->join('right', $join, $on);
        return $this;
    }

    /**
     * 设置表关联(inner)
     * @param $join mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function innerJoin($join, $on = null): TableCached
    {
        $this->statement->join('inner', $join, $on);
        return $this;
    }

    /**
     * 设置表关联(outer)
     * @param $join mixed 要关联的表
     * @param mixed $on 关联条件
     * @return $this
     */
    public function outerJoin($join, $on = null): TableCached
    {
        $this->statement->join('outer', $join, $on);
        return $this;
    }

    /**
     * 只设置关联条件
     * @param $on mixed 关联条件
     * @return $this
     */
    public function on($on): TableCached
    {
        $this->statement->on($on);
        return $this;
    }

    /**
     * 设置本次查询要进行唯一过滤
     */
    public function distinct(): TableCached
    {
        $this->statement->distinct = true;
        return $this;
    }

    /**
     * 设置本次操作的字段
     * @param $fields mixed
     * @return $this
     */
    public function fields($fields): TableCached
    {
        $this->statement->fields = $fields;
        return $this;
    }

    /**
     * 设置本次操作的搜索条件
     * @param $where mixed
     * @return $this
     */
    public function where($where): TableCached
    {
        $this->statement->where = $where;
        return $this;
    }

    /**
     * 设置本次操作的排序
     * @param $orderBy mixed
     * @return $this
     */
    public function orderBy($orderBy): TableCached
    {
        $this->statement->orderBy = $orderBy;
        return $this;
    }

    /**
     * 设置本次操作的分组
     * @param $groupBy mixed
     * @return $this
     */
    public function groupBy($groupBy): TableCached
    {
        $this->statement->groupBy = $groupBy;
        return $this;
    }

    /**
     * 设置本次操作的Having条件
     * @param $having mixed
     * @return $this
     */
    public function having($having): TableCached
    {
        $this->statement->having = $having;
        return $this;
    }

    /**
     * 设置本次操作的分页
     * @param $limit mixed
     * @return $this
     */
    public function limit($limit): TableCached
    {
        $this->statement->limit = $limit;
        return $this;
    }

    // 是否临时禁止缓存
    private $temporaryUnCache = false;

    /**
     * 临时禁止缓存,只禁止一次
     */
    public function disableCache(): TableCached
    {
        $this->temporaryUnCache = true;
        return $this;
    }

    /**
     * 检查是否允许使用Cache
     */
    private function enabled(): bool
    {
        // 如果本表强制要求文件缓存
        if ($this->fileCache) {
            return true;
        }

        // 如果已经临时禁止了缓存
        if ($this->temporaryUnCache or (isset($_GET['datacache']) and $_GET['datacache'] == 'false')) {
            // 下次不再临时禁止
            $this->temporaryUnCache = false;
            return false;
        }

        // 检查缓存是否开启
        return cache('Data')->enabled();
    }

    /**
     * 重载query方法
     * @param string $sql SQL语句
     * @param array|string $bind 要绑定的参数
     * @return mixed 查询数据
     * @throws MysqlException
     */
    public function query(string $sql, $bind = [])
    {
        $sql = trim($sql);

        // 执行前置 钩子
        $ret = $this->before('Query', $sql, $bind);
        if ($ret === self::INTERRUPT) {
            return null;
        }
        list($sql, $bind) = $ret;

        //修正查询参数
        $this->statement->query($sql, $bind);

        // 如果允许多表访问,直接通过
        if ($this->enableMulti()) {
            // 调用 父类的 查询语句获取结果, 执行后置钩子进行转换
            return $this->after('Query', self::query2());
        }

        // 取表名
        $table = $this->statement->getTable();
        $tables = count($table);

        // 如果未识别出表名,有可能是库操作
        if (!$tables) {
            // 调用 父类的 查询语句获取结果, 执行后置钩子进行转换
            return $this->after('Query', self::query2());
        }

        // 如果识别出多个表名
        if ($tables > 1) {
            trigger_error('查询语句中不允许同时指定多个表名:' . json($tables), E_USER_ERROR);
        }

        // 只有一个表
        $table = $table[0];

        // 如果语句中的表名与初始化时不同,则错误
        if ($table != $this->tableName) {
            trigger_error('表名不匹配:' . $this->tableName . ':' . $this->statement->getSql(), E_USER_ERROR);
        }

        // 调用 父类的 查询语句获取结果, 执行后置钩子进行转换
        return $this->after('Query', self::query2());
    }

    /**
     * 执行查询
     * 调用父类的查询,参数为语句(Statement),之后清除保存的各种状态
     * @return array
     */
    private function queryStatementAndClear()
    {
        $result = parent::queryStatement($this->statement);
        $this->clear();

        return $result;
    }

    // 本次查询是否从缓存中取回来的
    private $returnFromCache = false;

    /**
     * 处理查询操作,被query,select2,exist调用
     * @return array
     * @throws MysqlException
     */
    private function query2()
    {
        $this->returnFromCache = false;

        // 语句真正被创建
        $this->statement->create();

        // 取出语句的SQL,用于日志
        $sql = $this->statement->getSql();

        // 检查配置开关
        if (!$this->enabled()) {
            return self::queryStatementAndClear();
        }

        // 重构表名列表
        $tables = $this->statement->getTable();

        // 如果无法解析表名,不缓存,直接返回查询结果
        if (!$tables or !count($tables)) {
            debug(' Unknown table name');
            return self::queryStatementAndClear();
        }

        // 这句是用来载入类文件的
        $cache = cache('Data');
        if ($this->fileCache) {
            $cache = CacheFile::instance();
        }

        // 从缓存中取
        $data = $cache->get($sql);

        // 找到
        if ($data !== CacheFactory::NOT_FOUND) {
            $this->returnFromCache = true;
            return $data;
        }

        // 从数据库中取
        $data = self::queryStatementAndClear();

        // 设置到缓存中,并与每一个表关联
        foreach ($tables as $name) {
            $cache->set($name, $sql, $data);
        }
        return $data;
    }

    /**
     * 执行查询语句，并根据结果数量分别处理
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @param $limit mixed
     * @return array 二维数组(一维无键，二维有键)
     * @throws MysqlException
     */
    private function select2($fields = false, $where = null, $orderBy = null, $limit = null): array
    {
        //合并请求参数
        $statement = $this->statement;
        $statement->select($fields, $where, $orderBy, $limit);

        // 执行前置 钩子
        $ret = $this->before('Select', $statement);
        if ($ret === self::INTERRUPT) {
            return [];
        }

        //修正查询参数
        list($this->statement) = $ret;

        // 查询结果
        $ret = self::query2();

        // 清除Statement缓存
        $this->clear();

        // 执行后置钩子
        return $this->after('Select', $ret);
    }

    /**
     * 返回一条SQL语句
     *
     * @param string|null $fields
     * @param string $where
     * @param string $orderBy
     * @param string $limit
     * @return string
     * @throws MysqlException
     */
    public function sql(string $fields = null, string $where = null, string $orderBy = null, string $limit = null): string
    {
        $this->statement->select($fields, $where, $orderBy, $limit);

        // 语句真正被创建
        $this->statement->create();

        // 返回相应的SQL语句
        return $this->statement->getSql();
    }

    /**
     * 获取表结构信息
     * 这个不缓存,不受多表限制,直接调用底层查询
     * @param $name string|bool
     * @return array
     * @throws MysqlException
     */
    protected function getMeta(string $name = null): array
    {
        return self::query($this->db->createDesc($name ? $name : $this->tableName));
    }

    /**
     * 获取表的索引信息
     * @param string|null $name
     * @return array
     * @throws MysqlException
     */
    protected function getIndex(string $name = null): array
    {
        return self::query($this->db->createIndex($name ? $name : $this->tableName));
    }

    /**
     * 获取当前数据库中所有表名
     * @return array
     * @throws MysqlException
     */
    public function showTables(): array
    {
        return self::query($this->db->createShowTables());
    }

    /**
     * 获取默认数据库配置
     * @return string
     */
    private function defaultDatabase(): ?string
    {
        $default = configDefault(null, 'database', '_default', 'read', 'database');
        if (!$default) {
            trigger_error('尚未配置默认数据库(database|_default|read|database)', E_USER_ERROR);
        }

        return $default;
    }

    /**
     * 获取默认数据库的信息
     * @return array
     * @throws MysqlException
     */
    public function databaseInfo(): array
    {
        $default = $this->defaultDatabase();
        return self::query($this->db->createDatabaseInfo($default));
    }

    /**
     * 获取默认数据库的全部表的详细信息
     * @return array
     * @throws MysqlException
     */
    public function tablesStatus(): array
    {
        $default = $this->defaultDatabase();

        return self::query($this->db->createTablesStatus($default));
    }

    /**
     * 返回查询结果
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @param $limit mixed
     * @return array 二维数组(一维无键，二维有键)
     * @throws MysqlException
     */
    public function selectArray($fields = null, $where = null, $orderBy = null, $limit = null): array
    {
        return self::select2($fields, $where, $orderBy, $limit);
    }

    /**
     * 返回查询句柄
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @param $limit mixed
     * @return \PDOStatement
     * @throws MysqlException
     */
    public function selectHandle($fields = null, $where = null, $orderBy = null, $limit = null): ?\PDOStatement
    {
        // 执行前置钩子
        $ret = $this->before('Select', $fields, $where, $orderBy, $limit);
        if ($ret === self::INTERRUPT) {
            return null;
        }

        //修正请求参数
        list($fields, $where, $orderBy, $limit) = $ret;

        $this->statement->selectHandle($fields, $where, $orderBy, $limit);
        $this->statement->create();

        // 查询结果, 执行后置钩子
        return $this->after('Select', parent::queryHandle($this->statement));
    }

    /**
     * 重载execute方法,允许指定是否使用缓存,默认是使用缓存
     * @param string $sql 要执行的SQL语句
     * @param array|string $bind 要绑定的参数
     * @return mixed 执行结果
     * @throws MysqlException
     */
    public function execute(string $sql, $bind = [])
    {
        $sql = trim($sql);

        // 执行前置钩子
        $ret = $this->before('Execute', $sql, $bind);

        //如果被前置钩子阻止
        if ($ret === self::INTERRUPT) {
            return null;
        }

        //前置钩子修正参数
        list($sql, $bind) = $ret;

        //准备语句
        $this->statement->execute($sql, $bind);

        //要记录日志的数据
        $data = ['sql' => $sql, 'bind' => $bind];

        // 如果允许多表访问,直接通过
        if ($this->enableMulti()) {
            // 执行具体删除操作
            $result = self::log('execute', $data, '*', '', false, false);

            // 获取执行结果,执行后置钩子
            return $this->after('Execute', $result);
        }

        // 识别语句中的表名
        $tables = $this->statement->getTable();

        // 未能识别出表名,直接通过,这有可能是Desc/commit/rollback/begin
        if (!$tables or count($tables) == 0) {
            // 执行具体删除操作
            $result = self::log('execute', $data, '*', '', false, false);
            // 获取执行结果,执行后置钩子
            return $this->after('Execute', $result);
        }

        // 如果识别出多个表名
        if (count($tables) > 1) {
            trigger_error('执行语句中不允许同时指定多个表名', E_USER_ERROR);
        }

        // 取表名
        $table = $tables[0];

        // 如果语句中的表名与初始化时不同,则错误
        if ($table != $this->tableName) {
            trigger_error('表名不匹配:' . $this->tableName . ':' . $sql, E_USER_ERROR);
        }

        // 执行具体删除操作
        $result = self::log('execute', $data, '*', '', false, false);

        // 获取执行结果,执行后置钩子
        return $this->after('Execute', $result);
    }

    /**
     * 记录数据库操作日志
     * @param $operation string 操作名称
     * @param array $data 操作数据
     * @param string $fields 字段列表
     * @param mixed $where 条件
     * @param bool $before 是否保存之前数据
     * @param bool $after 是否保存之后 数据
     * @return bool
     * @throws MysqlException
     */
    private function log(string $operation, array $data, string $fields = '*', $where = '', bool $before = true, bool $after = true): bool
    {

        //检查是否需要记录数据库日志
        $operationLog = self::logEnabled();
        if (!$operationLog) {
            //不需要记录,直接执行
            return $this->execute2();
        }

        //如果需要保存之前数据
        if ($before) {
            //保存旧的statement对象
            $old = clone $this->statement;

            //查询修改前数据
            $data['before'] = $this->select2($fields, $where);

            //恢复statement对象
            $this->statement = clone $old;
        }

        //执行原始操作
        $result = $this->execute2();

        //如果需要记录操作后数据
        if ($after) {
            //查询修改后数据
            $data['after'] = $this->select2($fields, $where);
        }

        //记录日志
        call_user_func($operationLog, $this->tableName, $operation, $data);

        return $result;
    }

    /**
     * 执行操作
     * 调用父类的执行操作,参数为语句(Statement),之后清除各种保存的状态
     * @return bool
     */
    private function executeStatementAndClear(): bool
    {
        $result = parent::executeStatement($this->statement);
        $this->clear();
        return $result;
    }

    /**
     * 具体执行语句
     * @return bool
     * @throws MysqlException
     */
    private function execute2(): bool
    {
        //创建语句
        $this->statement->create();

        // 检查缓存配置开关
        if (!$this->enabled()) {
            return self::executeStatementAndClear();
        }

        // 重构相关表名
        $tables = $this->statement->getTable();

        // 如果可以解析表名 清除相关缓存
        if ($tables and count($tables)) {
            if ($this->fileCache) {
                $cache = CacheFile::instance();
            } else {
                $cache = cache('Data');
            }

            // 清除相关表中的所有缓存.因为可能影响到
            foreach ($tables as $name) {
                $cache->clear($name);
            }
        }

        return self::executeStatementAndClear();
    }

    /**
     * 根据配置信息,是否向记录中附加created/updated字段的值
     *
     * @param array $row
     * @return array
     */
    private
    function appendAutoField(array $row): array
    {
        // 如果不允许自动附加两个字段的话
        if (!$this->enableAutoField()) {
            return $row;
        }

        // 如果未指定创建时间字段,则自动指定为当前时间
        if (!isset($row['created'])) {
            $row['created'] = date('Y-m-d H:i:s');
        }

        // 如果未指定修改时间字段,则自动 指定为当前时间
        if (!isset($row['updated'])) {
            $row['updated'] = date('Y-m-d H:i:s');
        }

        return $row;
    }

    /**
     * Replace的 经过前置钩子之后 的处理
     * @param $row array 要插入的数据
     * @return int 行编号
     * @throws MysqlException
     */
    private function processReplace(array $row): int
    {
        // 执行具体插入操作
        $this->statement->replace($row);

        //检查是否需要记录数据库日志
        $operationLog = self::logEnabled();

        $before = '';
        //如果需要记录日志,记录操作前数据
        if ($operationLog) {
            $old = clone $this->statement;
            $before = self::select2('*', $row);
            $this->statement = clone $old;
        }

        //执行操作
        self::execute2();

        // 取插入后的记录的ID
        $id = $this->getInsertedId();
        if (!$id) {
            $id = $row['id'];
        }

        //如果需要记录日志,记录操作后数据
        if ($operationLog) {
            $after = self::select2('*', $row);
            call_user_func($operationLog, $this->tableName, 'replace', ['data' => $row, 'before' => $before, 'after' => $after]);
        }

        // 后置钩子
        return $this->after('Insert', 'Replace', $id);
    }

    /**
     * 具体执行Insert Ignore 并返回 数据行的编号
     * @param array $row 要插入的行
     * @return int
     * @throws MysqlException
     */
    private function processInsertIgnore2(array $row): int
    {
        self::execute2();

        // 获取新记录的主键
        $id = $this->getInsertedId();
        if ($id) {
            //已经有ID了,什么都不做
            return intval($id);
        }

        if (isset($row['id'])) {
            // 取记录中的ID
            return intval($row['id']);
        }

        //已经存在时返回0
        return 0;
    }

    /**
     * InsertIgnore的 经过前置钩子之后 的处理
     * @param $row array 要插入的数据
     * @return int 行编号
     * @throws MysqlException
     */
    private function processInsertIgnore(array $row): int
    {
        //准备语句
        $this->statement->insertIgnore($row);

        //插入并获取结果编号
        $id = self::processInsertIgnore2($row);

        //检查是否需要记录数据库日志
        $operationLog = self::logEnabled();
        if (!$operationLog) {
            // 执行后置钩子
            return $this->after('Insert', 'InsertIgnore', $id);
        }

        //判断插入是否成功
        if ($id) {
            //重新取出插入的数据
            $after = $this->select2('*', $id);

            //记录日志
            call_user_func($operationLog, $this->tableName, 'insertIgnore', ['data' => $row, 'after' => $after]);
        } else {
            //记录日志
            call_user_func($operationLog, $this->tableName, 'insertIgnore', ['data' => $row, 'id' => '插入失败']);
        }

        // 执行后置钩子
        return $this->after('Insert', 'InsertIgnore', $id);
    }

    /**
     * Insert的 经过前置钩子之后 的处理
     * @param $row array 要插入的数据
     * @return int 行编号
     * @throws MysqlException
     */
    private function processInsertOperation(array $row): int
    {
        //准备语句
        $this->statement->insert($row);

        //检查是否需要记录数据库日志
        $operationLog = self::logEnabled();

        if (!$operationLog) {
            //不需要记录,直接执行
            $this->execute2();

            // 获取插入的记录的ID,执行后置钩子
            return $this->after('Insert', 'Insert', $this->getInsertedId());
        }

        //执行原始操作
        $this->execute2();

        //获取插入的记录的ID
        $id = $this->getInsertedId();

        //判断插入是否成功
        if ($id) {
            //重新取出插入的数据
            $after = $this->select2('*', $id);

            //记录日志
            call_user_func($operationLog, $this->tableName, 'insert', ['data' => $row, 'after' => $after]);
        } else {
            //记录日志
            call_user_func($operationLog, $this->tableName, 'insert', ['data' => $row, 'id' => '插入失败']);
        }

        // 获取插入的记录的ID,执行后置钩子
        return $this->after('Insert', 'Insert', $id);
    }

    /**
     * Replace,Insert,InsertIgnore的后继处理
     * @param $operator string 操作符,Insert/Replace/InsertIgnore
     * @param $row array 要插入的数据
     * @return mixed|null|string
     * @throws MysqlException
     */
    private function afterInsertReplaceInsertIgnore(string $operator, array $row)
    {
        // 执行具体插入操作
        if ($operator == 'Insert') {
            return self::processInsertOperation($row);
        }

        // 创建"忽略插入"
        if ($operator == 'InsertIgnore') {
            return self::processInsertIgnore($row);
        }

        //处理Replace
        if ($operator == 'Replace') {
            return self::processReplace($row);
        }

        trigger_error('不应该到达这里:' . $operator, E_USER_ERROR);
        return null;
    }

    /**
     * 对应MYSQL的REPLACE语句,有则修改,无则插入
     * @param array $row
     * @return number|string
     * @throws MysqlException
     */
    public function replace(array $row)
    {
        // 这两个字段是表必备字段,并自动更新
        $row = self::appendAutoField($row);

        // 执行前置钩子
        $ret = $this->before('Insert', 'Replace', $row);
        if ($ret === self::INTERRUPT) {
            return null;
        }
        $row = array_pop($ret);

        //Insert/Replace/InsertIgnore的后继处理
        return self::afterInsertReplaceInsertIgnore('Replace', $row);
    }

    /**
     * 单行插入
     * @param array [1] $row  <列名>=><值>
     * @return int
     * @throws MysqlException
     */
    public function insert(array $row): ?int
    {
        // 这两个字段是表必备字段,并自动更新
        $row = array_filter(self::appendAutoField($row));

        // 执行前置钩子
        $ret = $this->before('Insert', 'Insert', $row);
        if ($ret === self::INTERRUPT) {
            return null;
        }
        $row = array_pop($ret);

        //Insert/Replace/InsertIgnore的后继处理
        return self::afterInsertReplaceInsertIgnore('Insert', $row);
    }

    /**
     * 多行插入
     * @param array $rows 要插入的多行数据
     * @return bool|null
     * @throws MysqlException
     */
    public function inserts(array $rows): ?bool
    {
        //逐行执行前置钩子
        foreach ($rows as $key => $row) {
            //增加自动字段
            $row = self::appendAutoField($row);

            //执行前置钩子
            $ret = $this->before('Insert', 'Insert', $row);

            //前置钩子失败,不执行插入操作
            if ($ret === self::INTERRUPT) {
                return null;
            }
            $rows[$key] = array_pop($ret);
        }

        //记录信息
        $this->statement->inserts($rows);

        //执行操作,并记录日志
        self::log('inserts', $rows, '', '', false, false);

        //多行插入,无法执行后置钩子
        return true;
    }

    /**
     * 无则插入,有则忽略
     *
     * @param array $row
     * @return number string
     * @throws MysqlException
     */
    public function insertIgnore(array $row)
    {
        // 这两个字段是表必备字段,并自动更新
        $row = self::appendAutoField($row);

        // 执行前置钩子
        $ret = $this->before('Insert', 'InsertIgnore', $row);
        if ($ret === self::INTERRUPT) {
            return null;
        }
        $row = array_pop($ret);

        //Insert/Replace/InsertIgnore的后继处理
        return self::afterInsertReplaceInsertIgnore('InsertIgnore', $row);
    }

    /**
     * 删除表中的部分数据
     *
     * @param mixed $where 请参考createWhere
     * @return int
     * @throws MysqlException
     */
    public function delete($where): ?int
    {
        // 调用前置钩子
        $ret = $this->before('Delete', $where);

        //如果前置钩子阻止
        if ($ret === self::INTERRUPT) {
            return null;
        }

        //钩子对参数进行修正
        $where = array_pop($ret);

        //创建语句
        $this->statement->delete($where);

        // 执行具体删除操作
        $result = self::log('delete', ['where' => $where], '*', $where, true, false);

        // 如果存在后置钩子,则调用
        return intval($this->after('Delete', $result));
    }

    /**
     * 删除表中全部数据
     * @return int
     * @throws MysqlException
     */
    public function deleteAll(): int
    {
        return self::delete('1=1');
    }

    /**
     * 获取一列数据
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @param $limit mixed
     * @return array
     * @throws MysqlException
     */
    public function col($fields, $where = null, $orderBy = null, $limit = null): array
    {
        // 按Select进行查询
        $table = self::selectArray($fields, $where, $orderBy, $limit);

        // 如果没有查到
        if (!$table) {
            return [];
        }

        // 保存返回值
        $ret = [];

        // 取所有的列名
        $keys = array_keys($table[0]);

        // 取第一个列名
        $key = $keys[0];

        // 逐个循环,取出一列的值
        foreach ($table as $row) {
            $ret[] = $row[$key];
        }

        // 返回一列
        return $ret;
    }

    /**
     * 获取一个整数,参考下面的get
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @return int
     * @throws MysqlException
     */
    public function getInt($fields = null, $where = null, $orderBy = null): int
    {
        return intval(self::get($fields, $where, $orderBy));
    }

    /**
     * 获取满足条件的记录的ID值
     * @param $where mixed
     * @param mixed $orderBy
     * @return int
     * @throws MysqlException
     */
    public function getId($where, $orderBy = null): int
    {
        return self::getInt('id', $where, $orderBy);
    }

    /**
     * 获取一个浮点数,参考下面的get
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @return float
     * @throws MysqlException
     */
    public function getFloat($fields = null, $where = null, $orderBy = null): float
    {
        return (float)self::get($fields, $where, $orderBy);
    }

    /**
     * 获取一个值
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @return mixed 单值
     * @throws MysqlException
     */
    public function get($fields, $where = null, $orderBy = null)
    {
        // 按SELECT进行查询
        $table = self::selectArray($fields, $where, $orderBy, 1);

        // 如果没有查到
        if (!$table or !count($table)) {
            return null;
        }

        // 取第一行
        $row = $table[0];

        // 取所有的键名
        $keys = array_keys($row);

        // 取第一列的值
        return $row[$keys[0]];
    }

    /**
     * 获取一行数据
     * @param $fields mixed
     * @param $where mixed
     * @param $orderBy mixed
     * @return null|Row
     * @throws MysqlException
     */
    public function row($fields = null, $where = null, $orderBy = null)
    {
        // 按整个SELECT进行查询
        $table = self::selectArray($fields, $where, $orderBy, "1");

        // 如果有,返回第一行
        if ($table) {
            return $table[0];
        }

        return null;
    }

    /**
     * 内部方法 对指定字段进行增减操作
     *
     * @param string $operator '+'/'-'
     * @param mixed $fields 请参考 createField
     * @param mixed $where 请参考 _createWhere
     * @param float $diff 增加或减少的数值
     * @return int|false 影响的行数
     * @throws MysqlException
     */
    private function crease(string $operator = '+', $fields, $where, float $diff): int
    {
        // 执行前置钩子
        $ret = $this->before('Crease', $operator, $fields, $where, $diff);

        //如果被前置钩子阻止
        if ($ret === self::INTERRUPT) {
            return 0;
        }

        //钩子对参数进行修正
        list($operator, $fields, $where, $diff) = $ret;

        //创建语句
        $this->statement->crease($operator, $fields, $where, $diff);

        // 执行具体增减
        $result = self::log('crease', ['fields' => $fields, 'where' => $where, 'diff' => $diff], $fields, $where, true, true);

        // 执行后置钩子
        return intval($this->after('Crease', $result));
    }

    /**
     * 对指定字段进行加一操作
     *
     * @param mixed $fields 请参考 createField
     * @param mixed $where 请参考 _createWhere
     * @param int|float $diff 增加的数值
     * @return int 影响的行数
     * @throws MysqlException
     */
    public function increase($fields, $where, float $diff = 1): int
    {
        return intval(self::crease('+', $fields, $where, $diff));
    }

    /**
     * 对指定字段进行减一操作
     *
     * @param mixed $fields 请参考 createField
     * @param mixed $where 请参考 _createWhere
     * @param int|float $diff 增加的数值
     * @return int 影响的行数
     * @throws MysqlException
     */
    public function decrease($fields, $where, float $diff = 1): int
    {
        return self::crease('-', $fields, $where, $diff);
    }

    /**
     * 修改表中的部分数据
     *
     * @param array [1] $row <列名>=><值>
     * @param mixed $where 请参考createWhere
     * @return mixed
     * @throws MysqlException
     */
    public function update(array $row, $where = null)
    {
        // updated字段是表必备字段,并自动更新
        if ($this->enableAutoField()) {
            $row['updated'] = date('Y-m-d H:i:s');
        }

        // 执行前置钩子
        $ret = $this->before('Update', $row, $where);
        if ($ret === self::INTERRUPT) {
            return null;
        }

        //修正请求参数
        list($row, $where) = $ret;

        // 记录修改操作
        $this->statement->update($row, $where);

        //是否需要记录文本日志
        if (self::logEnabled()) {
            //复制原Statement,因为下面的Count要用.
            $statement = clone $this->statement;

            //查看满足条件的记录数
            $count = $this->count($where);

            //恢复Statement
            $this->statement = $statement;

            //记录日志,如果影响行数过多,则不记录之前,之后的数据
            $result = self::log('update', ['data' => $row, 'where' => $where], '*', $where, $count <= 20, $count <= 20);
        } else {
            $result = $this->execute2();
        }

        // 执行后置钩子
        return $this->after('Update', $result);
    }

    /**
     * 统计满足条件的记录数
     * @param mixed $where 请参考createWhere
     * @return int
     * @throws MysqlException
     */
    public function count($where = null): int
    {
        // 按单值进行查询
        return intval(self::get(['count(*)' => 'cnt'], $where));
    }

    /**
     * 判断是否存在满足条件的记录
     *
     * @param mixed $where
     * @return bool
     * @throws MysqlException
     */
    public function exist($where = false): bool
    {
        $this->statement->exist($where);
        $ret = self::query2();
        return $ret[0]['cnt'] ? true : false;
    }

    /**
     * 判断是否不存在满足条件的记录
     * @param bool $where 条件
     * @return bool
     * @throws MysqlException
     */
    public function notExist($where = false): bool
    {
        return !self::exist($where);
    }

    /**
     * 累加指定字段
     *
     * @param mixed $field 字段
     * @param mixed $where 条件
     * @return float
     * @throws MysqlException
     */
    public function sum($field, $where = null): float
    {
        return floatval(self::get(['sum(' . $field . ')' => 'sum'], $where));
    }

    /**
     * 调用相应数据库的类,对字段名处理
     * @param $field string
     * @return string
     */
    public function markField(string $field): string
    {
        return $this->db->markField($field);
    }

    /**
     * 调用相应数据库的类,对字段列表处理
     * @param $fields
     * @return string
     */
    public function createFields($fields): string
    {
        return $this->db->createFields($fields);
    }
}