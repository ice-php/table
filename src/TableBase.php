<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 直接访问数据库的基础表类
 * name获取当前表名
 * lock/unlock写锁定
 * escape转义
 * begin/commit/rollback 提供事务功能
 * queryHandle 快速查询大量数据
 * getForeignKey查看指定字段的外键信息
 */
abstract class TableBase
{
    // 某一类型的数据库语法对象
    protected $db;

    // 表名
    protected $tableName;

    // 表的别名(如果没有，就是表名）
    protected $alias;

    /**
     * 连接句柄,用于读操作
     *
     * @var \PDO
     */
    private $handleRead;

    /**
     * 连接句柄,用于写操作
     *
     * @var \PDO
     */
    private $handleWrite;

    /**
     * 构造方法
     * @param string $alias
     * @throws MysqlException 表名错误
     */
    protected function __construct(string $alias)
    {
        // 取数据库类型,默认为mysql
        $sql = configDefault('mysql', 'system', 'sql');

        if ($sql == 'mysql') {
            // 只支持这个
            $this->db = Mysql::instance();
        } elseif ($sql == 'oracle') {
            trigger_error('不支持的数据库类型:Oracle');
        } elseif ($sql == 'sqlserver') {
            trigger_error('不支持的数据库类型:SQLServer');
        } else {
            trigger_error('不认识的数据库类型:'.$sql);
        }

        // 记录表的别名
        $this->alias = $alias;

        // 检查表名,并规范化
        $config = configDefault('', 'database', $alias);

        // 查找表的实际名称
        if ($config and isset($config['table'])) {
            $table = $config['table'];
        } else {
            $table = $alias;
        }

        // 表名规范化,加上前缀,后缀
        $prefix = configDefault('', 'database', '_prefix');
        $suffix = configDefault('', 'database', '_suffix');
        $this->tableName = $prefix . $this->db->createTableName($table) . $suffix;
    }

    /**
     * 查看是否允许多表关联访问
     * @return boolean
     */
    protected function enableMulti(): bool
    {
        return configDefault(false, 'database', '_enable_multi');
    }

    /**
     * 获取当前表的名称
     * @return string
     */
    public function name(): string
    {
        return $this->tableName;
    }

    /**
     * 获取当前表的别名
     * @return string
     */
    public function alias(): string
    {
        return $this->alias;
    }

    /**
     * 出于读目的,连接数据库,返回连接句柄
     * 根据当前表别名,从配置文件读取连接参数
     * 延迟连接,因为有可能缓存,可以不访问数据库
     * 支持读写分离
     *
     * @return \PDO 句柄
     * @throws MysqlException
     */
    private function connectRead(): \PDO
    {
        // 如果当前在事务中,读操作也从写服务器执行
        if (self::$transaction) {
            return $this->connectWrite();
        }

        // 如果已经连接,直接返回句柄
        if (!$this->handleRead) {
            // 获取连接句柄
            $this->handleRead = $this->db->connect($this->alias, 'read');
        }
        return $this->handleRead;
    }

    /**
     * 出于写目的,连接数据库,返回连接句柄
     * 根据当前表别名,从配置文件读取连接参数
     * 延迟连接,因为有可能缓存,可以不访问数据库
     * 支持读写分离
     * @return \PDO
     * @throws MysqlException
     */
    private function connectWrite(): \PDO
    {
        $this->handleWrite = $this->db->connect($this->alias, 'write');
        return $this->handleWrite;
    }

    /**
     * 快速 版插入(不记录日志,不记录调试)
     * @param array $row 要插入的数据
     * @return int 新插入数据的ID
     * @throws MysqlException
     */
    public function insertFast(array $row): int
    {
        $connect = $this->connectWrite();

        $count = count($row);
        $prepare = implode(',', array_keys($row));
        $params = trim(str_repeat('?,', $count), ',');
        $sql = 'INSERT' . ' INTO ' . $this->name() . '(' . $prepare . ') VALUES(' . $params . ')';
        $statement = $connect->prepare($sql);
        $statement->execute(array_values($row));
        return intval($connect->lastInsertId());
    }

    /**
     * 快速版执行,不记录日志
     * @param $sql string 要执行的语句
     * @param array|string $bind 要绑定的参数
     * @return int|bool 影响的行数
     * @throws MysqlException|TableException
     */
    public function executeFast(string $sql, $bind = [])
    {
        if (!$bind) {
            return $this->connectWrite()->exec($sql);
        }
        return $this->bindExecute($this->connectWrite(), $sql, $bind);
    }

    /**
     * 快速版查询,不记录日志,返回句柄
     * @param $sql string 要查询的语句
     * @param array|string $bind 要绑定的参数
     * @return \PDOStatement
     * @throws MysqlException|TableException
     */
    public function queryFastHandle(string $sql, $bind = []): \PDOStatement
    {
        if (!$bind) {
            return $this->connectWrite()->query($sql, \PDO::FETCH_ASSOC);
        }

        $stmt = $this->connectWrite()->prepare($sql);
        $ret = $stmt->execute($bind);

        //通常是参数绑定问题
        if (!$ret) {
            throw new TableException('PDO语句参数绑定错误:' . $stmt->errorCode(), TableException::PDO_BIND_ERROR);
        }
        return $stmt;
    }

    /**
     * 快速版查询,只记录操作日志,不记录结果日志
     * @param $sql string 要查询的语句
     * @param array|string $bind 要绑定的参数
     * @return array
     * @throws MysqlException|TableException
     */
    public function queryFast(string $sql, $bind = []): array
    {
        $connect = $this->connectWrite();

        // 先记日志,以免错误导致无日志
        FileLog::instance()->sqlBefore('queryFast', $sql);

        //快速查询不记录查询结果日志
        if (!$bind) {
            return $connect->query($sql, \PDO::FETCH_ASSOC)->fetchAll();
        }

        //带参数绑定
        return $this->bindQuery($connect, $sql, $bind);
    }

    /**
     * 对表进行写锁定(或指定读锁)
     * @param $level string read|write
     * @return TableBase
     * @throws TableException|MysqlException
     */
    public function lock(string $level = 'write'): TableBase
    {
        //读/写模式
        $level = strtolower($level);
        if (!in_array($level, ['read', 'write'])) {
            throw new TableException('锁表类型只能是READ/WRITE:' . $level, TableException::LOCK_TYPE_ERROR);
        }

        $this->db->lock($this->tableName, $level);
        return $this;
    }

    /**
     * 解除表锁
     * @return TableBase
     * @throws MysqlException
     */
    public function unlock(): TableBase
    {
        $this->db->unlock($this->tableName);
        return $this;
    }

    /**
     * 转义
     * @param $str number|string 要转义的字符串
     * @return string 转义结果
     */
    public function escape($str): string
    {
        return Mysql::escape($str);
    }

    /**
     * 获取配置中是否允许自动字段(created,updated)
     * @return bool
     */
    protected function enableAutoField(): bool
    {
        return boolval(configDefault(false, 'database', '_auto_field'));
    }

    /**
     * 判断是否需要记录 数据库记录日志
     * @return bool|array
     */
    protected function logEnabled()
    {
        //日志表名称数组
        $tables = configDefault([], 'log', 'noLogTables');
        if (!$tables) {
            return false;
        }

        //日志方法(callable)
        $method = configDefault(false, 'log', 'operationLog');
        if (!$method) {
            return false;
        }

        //当前表是日志表
        if (in_array($this->tableName, $tables)) {
            return false;
        }

        //返回日志方法
        return $method;
    }

    /**
     * 执行增删改,并记录日志
     * @param Statement $statement
     * @return bool
     * @throws TableException|MysqlException
     */
    protected function executeStatement(Statement $statement): bool
    {
        // 要执行的SQL语句必须是字符串
        if ($statement->isNull()) {
            throw new TableException('要执行的语句不能为空', TableException::EXECUTE_NULL);
        }

        // 记录开始时间
        $begin = timeLog();

        // 获取数据库句柄(只使用写服务器)
        $connect = $this->connectWrite();

        // 获取要执行的语句的Prepare和Params
        $prepare = $statement->getPrepare();
        $params = $statement->getParams();

        $noLog = in_array($this->tableName, configDefault([], 'log', 'noLogTables'));

        //先记录日志,以免出错崩溃
        if (!$noLog) {
            FileLog::instance()->sqlBefore('execute', $statement->getSql());
        }

        // 如果参数为空,就不必使用prepare模式了,直接执行
        if (empty($params)) {
            $result = $connect->exec($prepare);
        } elseif ($statement->isQuery()) {
            // 先预备,再附加参数进行执行
            $result = $this->bindExecute($connect, $prepare, $params);
        } else {
            // 先预备,再附加参数进行执行
            $stmt = $connect->prepare($prepare);
            $result = $stmt->execute($params);
            unset($stmt);
        }

        // 计算耗时
        $interval = timeLog($begin);

        // 记录耗时
        if (!$noLog) {
            FileLog::instance()->sqlAfter('afterExecute', $statement->getSql(), $result, $interval, $statement->getOperation());
        }

        // 输出调试信息:执行时间
        Debug::setSql('Execute', $prepare, $interval, $params, $statement->getSql());

        return boolval($result);
    }

    /**
     * 获取最后插入的记录的ID
     * @return int
     * @throws MysqlException
     */
    protected function getInsertedId(): int
    {
        return (int)$this->connectWrite()->lastInsertId();
    }

    /**
     * 执行查询并记录日志
     *
     * @param Statement $statement
     * @return mixed
     * @throws TableException|MysqlException
     */
    protected function queryStatement(Statement $statement)
    {
        // 要查询的SQL语句必须是字符串
        if ($statement->isNull()) {
            throw new TableException('要查询的语句不能为空', TableException::QUERY_NULL);
            //will throw exception
        }

        // 记录开始时间
        $begin = timeLog();

        // 获取要执行的语句的Prepare和Params
        $prepare = $statement->getPrepare();
        $params = $statement->getParams();

        //记录查询日志
        FileLog::instance()->sqlBefore('query', $statement->getSql());

        // 从读服务器进行读操作
        $connect = $this->connectRead();

        // 如果绑定参数为空,直接执行就好.
        if (empty($params)) {
            $ret = $connect->query($prepare);
            $result = $ret->fetchAll(\PDO::FETCH_ASSOC);
            unset($ret);
        } else {
            $result = $this->bindQuery($connect, $prepare, $params);
        }

        // 计算耗时
        $interval = timeLog($begin);

        // 记录耗时
        FileLog::instance()->sqlAfter('afterQuery', $statement->getSql(), $result, $interval, $statement->getOperation());

        // 记录调试信息
        Debug::setSql('Query', $prepare, $interval, $params, $statement->getSql());
        return $result;
    }

    /**
     * 返回查询语句句柄,不需要事先读取全部数据,最节省内存
     * 为 Table调用
     * @param Statement $statement
     * @return \PDOStatement
     * @throws MysqlException
     */
    public function queryHandle(Statement $statement): \PDOStatement
    {
        // 记录开始时间
        $begin = timeLog();

        // 获取要执行的语句的Prepare和Params
        $prepare = $statement->getPrepare();
        $params = $statement->getParams();

        // 先记日志,以免错误导致无日志
        FileLog::instance()->sqlBefore('queryHandle', $statement->getSql());

        // 从读服务器进行
        $connect = $this->connectRead();

        // 禁止缓存
        $connect->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

        // 如果绑定参数为空,直接执行就好.
        if (empty($params)) {
            $result = $connect->query($prepare);
        } else {
            $stmt = $connect->prepare($prepare);
            $stmt->execute($params);
            $result = $stmt;
        }

        // 查询完成,再次打开缓存
        $connect->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true);

        // 计算耗时
        $interval = timeLog($begin);

        // 记录耗时
        FileLog::instance()->sqlAfter('afterQueryHandle', $statement->getSql(), $result, $interval, $statement->getOperation());

        Debug::setSql('QueryHandle', $prepare, $interval, $params, $statement->getSql());
        return $result;
    }

    /**
     * 子类要实现此方法 执行SQL操作
     * @param $sql string 语句
     * @param array|string $bind 要绑定的参数
     * @return mixed
     */
    abstract public function execute(string $sql, $bind = []);

    /**
     * 子类要实现此方法 执行SQL查询
     * @param $sql string 语句
     * @return mixed
     */
    abstract public function query(string $sql);

    // 记录当前事务层次
    private static $transaction = 0;

    /**
     * 事务开始,自动解除自动提交
     * @return int
     * @throws MysqlException|TableException
     */
    public function begin(): int
    {
        if (self::enableMulti()) {
            throw new TableException('允许多表查询情况下无法使用事务:datbase/_enable_multi', TableException::TRANSACTION_IN_MULTI);
        }

        // 事务层数加1
        self::$transaction++;

        // 开始事务
        $this->connectWrite()->beginTransaction();

        return self::$transaction;
    }

    /**
     * 事务提交
     * @throws TableException|MysqlException
     */
    public function commit(): void
    {
        // 如果允许多数据库,无法事务
        if (self::enableMulti()) {
            throw new TableException('允许多表查询情况下无法使用事务:datbase/_enable_multi', TableException::TRANSACTION_IN_MULTI);
        }

        // 如果事务层次没了,不应该提交
        if (self::$transaction <= 0) {
            throw new TableException('不允许单独的事务提交(缺少事务开始)', TableException::COMMIT_WITHOUT_BEGIN);
        }

        // 嵌套层次减少
        self::$transaction--;

        // 发送数据库的提交
        $this->connectWrite()->commit();
    }

    /**
     * 事务回滚
     * @throws TableException|MysqlException
     */
    public function rollback(): void
    {
        // 允许多库时,无法回滚
        if (self::enableMulti()) {
            throw new TableException('允许多表查询情况下无法使用事务:datbase/_enable_multi', TableException::TRANSACTION_IN_MULTI);
        }

        // 嵌套层次没了
        if (self::$transaction <= 0) {
            throw new TableException('不允许单独的事务回滚(缺少事务开始)', TableException::ROLLBACK_WITHOUT_BEGIN);
        }

        // 嵌套层次减少
        self::$transaction--;

        // 数据库回滚
        $this->connectWrite()->rollBack();

        // 如果没有嵌套层次 没了,重新设置为自动提交
        if (!self::$transaction) {
            $this->connectWrite()->setAttribute(\PDO::ATTR_AUTOCOMMIT, 1);
        }
    }

    /**
     * 获取指定字段的外键信息（哪个表，哪个字段）
     *
     * @param string $name 表名
     * @return boolean|array|string
     */
    public function getForeignKey(string $name = null)
    {
        // 构造 查询语句
        $sql = $this->db->getCreate($name ? $name : $this->tableName);
        $result = $this->query($sql);

        // 分析结果
        $statement = $result[0]['Create Table'];
        $matched = preg_match_all('/CONSTRAINT\s\S*\sFOREIGN\sKEY\s\(`([^`]*)`\) REFERENCES `([^`]*)` \(`([^`]*)`\)/i', $statement, $matches, PREG_SET_ORDER);
        if (!$matched) {
            return false;
        }

        // 如果有多个
        $fks = [];
        foreach ($matches as $m) {
            $fks[$m[1]] = [
                'table' => $m[2],
                'field' => $m[3]
            ];
        }
        return $fks;
    }

    /**
     * 绑定查询,有错误 则抛出
     * @param \PDO $connect 数据库连接
     * @param $prepare string 查询语句(带占位符)
     * @param $params array 绑定参数
     * @return array
     * @throws TableException
     */
    private function bindQuery(\PDO $connect, string $prepare, array $params = []): array
    {
        $stmt = $connect->prepare($prepare);
        $ret = $stmt->execute($params);

        //通常是参数绑定问题
        if (!$ret) {
            throw new TableException('PDO语句参数绑定错误:' . $stmt->errorCode(), TableException::PDO_BIND_ERROR);
        }
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * 绑定执行,有错误则抛出
     * @param \PDO $connect 数据库连接
     * @param $prepare string 查询语句(带占位符)
     * @param $params array 绑定参数
     * @return bool
     * @throws TableException
     */
    private function bindExecute(\PDO $connect, string $prepare, array $params): bool
    {
        $stmt = $connect->prepare($prepare);
        $ret = $stmt->execute($params);

        //通常是参数绑定问题
        if (!$ret) {
            throw new TableException('PDO语句参数绑定错误:' . $stmt->errorCode(), TableException::PDO_BIND_ERROR);
        }
        return $ret;
    }
}