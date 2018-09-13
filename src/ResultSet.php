<?php
declare(strict_types=1);

namespace icePHP;
/**
 * 指定表之后的结果集,成员为Record类
 * User: Administrator
 * Date: 2017/10/17
 * Time: 16:59
 */
class ResultSet extends Result
{
    //本结果集中行对象的类名, 例:RUserInfo
    private $recordClass;

    //本结果集中表对象的类名,例:TUserInfo
    private $tableClass;

    /**
     * 保存原始数据的键
     * @var array
     */
    private $oldPrimaryKeys = [];

    //主键字段名
    private $primaryKey;

    /**
     * 开发人员不要调用此方法
     * 构造方法,由自己调用
     * SResultSet constructor.
     * @param string $tableName
     * @param array $data
     * @throws MysqlException|TableException
     */
    public function __construct(string $tableName, array $data)
    {
        //按SResult创建
        parent::__construct($tableName, $data);

        //表名,首页字母大写规范化
        $formatted = self::formatter($tableName, true);

        /**
         * 具体的表 类
         * @var $tableClass Table
         */
        $tableClass = $this->tableClass = 'T' . $formatted;


        //表的主键字段  这个肯定有
        $this->primaryKey = $tableClass::$primaryKey;

        //行对象类
        $this->recordClass = 'R' . $formatted;
        $class = $this->recordClass;

        //每个行,转换成Record的子类,具体行记录类
        foreach ($this->rows as $key => $row) {
            $this->rows[$key] = new $class($row);

            //保存原始数据的主键
            $this->oldPrimaryKeys[] = $row[$this->primaryKey];
        }
    }

    /**
     * 实例化,由关联关系自动生成
     * @param Result $result 结果集
     * @return ResultSet
     * @throws MysqlException|TableException
     */
    static public function instance(Result $result): ResultSet
    {
        return new self($result->tableName, $result->all());
    }

    //记录本结果集是否由hasMany/belongsToMany创建
    private $type;

    /**
     * @var array 记录关联配置
     */
    private $relation;

    /**
     * 记录结果集类型和关联配置
     * @param $type string hasMany/belongsToMany
     * @param $relation array 关联 配置
     * @return $this
     */
    public function _setType(string $type, array $relation)
    {
        $this->type = $type;
        $this->relation = $relation;
        return $this;
    }

    /**
     * 保存时,要每个行对象进行保存
     * 还要查看是否有需要删除的对象
     * @throws TableException|MysqlException
     */
    public function save(): void
    {
        $keys = [];

        $primaryKey = $this->primaryKey;

        //每个行对象进行保存
        foreach ($this as $record) {
            /*
             * @var SRecord
             */
            $record->save();

            //取行对象的主键值
            $keys[] = $record->{$primaryKey};
        }

        //要删除的记录的主键值
        $deletedKeys = array_diff($this->oldPrimaryKeys, $keys);

        //一对多时,在关联表中进行删除
        if ($this->type === 'hasMany') {
            /**
             * @var $tableClass Table
             */
            $tableClass = $this->tableClass;
            $tableClass::instance()->delete([$this->primaryKey => $deletedKeys]);
        } elseif ($this->type === 'belongsToMany') {
            //多对多关联时,删除 中间表
            $config = $this->relation;

            //配置信息
            $middleClass = $config['middle'];  //中间表
            $selfForeignKey = $config['selfForeignKey'];//中间表中与本表关联的键
            $targetForeignKey = $config['targetForeignKey'];//中间表中与对方 表关联的键
            $selfPrimaryValue = $config['selfPrimaryValue']; //主表主键值

            //在中间表中进行删除
            $middleClass::instance()->delete([$selfForeignKey => $selfPrimaryValue, $targetForeignKey => $deletedKeys]);
        }
    }

    /**
     * 向结果集中添加一个新记录
     * @param Record $record
     */
    public function add(Record $record): void
    {
        $class = $this->recordClass;
        $this->rows[] = new $class($record->toArray());
    }

    /**
     * 从结果集中删除一个记录,以后save时会自动从数据库中删除
     * @param mixed $record
     * @throws
     */
    public function remove(Record $record): void
    {
        //本结果集的表主键
        $primaryKey = $this->primaryKey;

        //要删除的主键值
        $deleted = [];

        //根据参数类型判断
        if ($record instanceof Record or $record instanceof Row) {
            //行对象类型
            $deleted[] = $record->$primaryKey;
        } elseif (!is_array($record)) {
            //简单类型
            $deleted[] = $record;
        } else {
            //数组,要逐个删除
            foreach ($record as $k => $r) {
                if ($r instanceof Record or $r instanceof Row) {
                    $deleted[] = $r->$primaryKey;
                } elseif (!is_array($r)) {
                    $deleted[] = $r;
                } else {
                    throw  new TableException('要移除的记录无法识别:' . json($r), TableException::WANT_REMOVE_UNKNOWN);
                }
            }
        }

        //在结果集中看是否需要删除
        foreach ($this->rows as $key => $row) {
            if (in_array($row->{$this->primaryKey}, $deleted)) {
                unset($this->rows[$key]);
            }
        }
    }


    /**
     * 将下划线分隔的名字,转换为驼峰模式
     *
     * @param string $name 下划线分隔的名字
     * @param bool $firstUpper 转换后的首字母是否大写
     * @return string
     */
    private static function formatter(string $name, bool $firstUpper = true): string
    {
        // 将表名中的下划线转换为大写字母
        $words = explode('_', $name);
        foreach ($words as $k => $w) {
            $words [$k] = ucfirst($w);
        }

        // 合并
        $name = implode('', $words);

        // 如果明确要求首字母小写
        if (!$firstUpper) {
            $name = lcfirst($name);
        }

        // 返回名字
        return $name;
    }
}