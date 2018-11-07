<?php
declare(strict_types=1);

namespace icePHP;

/**
 * ActiveRecord类的基类.
 * User: 蓝冰
 * Date: 2016/5/13
 * Time: 14:52
 */
abstract class Record
{
    /**
     * @var array 本行数据,初始化或载入时的值
     */
    protected $_data;

    /**
     * @var bool 本行数据是否已经从数据库中读取
     */
    protected $_loaded;

    /**
     * @var string 主键字段名
     */
    protected $_primaryKey;

    /**
     * @var string 表名(别名)
     */
    protected $_tableName;

    /**
     * @var string 本表的格式化名称(大写开头)
     */
    protected $_baseName;

    /**
     * @var Table 表对象
     */
    protected $_table;

    /**
     * 使用给定数据创建一个当前表的行对象
     * @param array|Row $data
     */
    public function __construct($data = null)
    {
        //构造表对象
        $this->_table = table($this->_tableName);

        //如果提供了数据,则设置记录值
        if ($data) {
            $this->set($data);
        }
    }

    /**
     * 批量设置记录值
     * @param $data mixed 可以是SRow,Array,或主键
     * @return $this
     */
    public function set($data): Record
    {
        //可以以SRow格式或数组格式提供数据
        if ($data instanceof Row) {
            $this->_data = $data->all();
        } elseif (is_array($data)) {
            $this->_data = $data;
        } elseif ($data) {
            $this->loadByPk($data);
        }

        //保存原始数据
        $this->_old = $this->_data;

        //执行读取数据后的钩子
        if ($this->_data) {
            $this->_data = $this->afterLoad($this->_data);
        }

        //将原始值赋值给属性
        if ($data) {
            foreach ($this->_data as $k => $v) {
                $this->$k = $v;
            }
        }
        return $this;
    }

    /**
     * 获取当前表名
     * @return string
     */
    public function tableName(): string
    {
        return $this->_tableName;
    }

    /**
     * 获取本记录的主键值
     * @return mixed
     */
    public function getPrimaryKeyValue()
    {
        return $this->_data[$this->_primaryKey];
    }

    //子类会实现此属性,全部字段名称
    protected static $_fields;

    /**
     * 获取本记录,要写入数据库的数据,经过钩子转换
     * @return array
     */
    private function dataForWrite(): array
    {
        $data = self::all();

        //执行写入前的钩子
        return $this->beforeSave($data);
    }

    /**
     * 返回本记录 当前数据
     * @return array
     */
    private function all(): array
    {
        $fields = static::$_fields;

        $data = [];
        foreach ($fields as $f) {
            if (isset($this->$f) and !is_null($this->$f)) {
                $data[$f] = $this->$f;
            }
        }

        return $data;
    }

    /**
     * 删除 当前记录
     */
    public function remove(): void
    {
        //没有主键字段,则不进行保存
        if (!$this->_primaryKey) {
            return;
        }

        //获取要写入的数据
        $data = self::dataForWrite();

        //本表的主键字段
        $pk = $this->_primaryKey;

        //如果没有主键字段值,则不操作
        if (!isset($data[$pk])) {
            return;
        }

        //一对一关联,要对应删除
        if (!empty($this->_hasOne)) {
            foreach ($this->_hasOne as $r) {
                $this->{$r['name']}->remove();
            }
        }

        //一对多关联,要逐个删除
        if (!empty($this->_hasMany)) {
            foreach ($this->_hasMany as $r) {
                $name = $r['name'];
                foreach ($this->$name as $subRecord) {
                    $this->$name->remove($subRecord);
                }
            }
        }

        //多对一关联,不动

        //多对多关联要逐个删除
        if (!empty($this->_belongsToMany)) {
            foreach ($this->_belongsToMany as $r) {
                $name = $r['name'];
                foreach ($this->$name as $subRecord) {
                    $this->$name->remove($subRecord);
                }
            }
        }

        //根据主键进行删除
        $this->_table->delete([$pk => $data[$pk]]);
        $this->_data = null;
        $this->_loaded = false;
    }

    /**
     * 保存数据
     * @return $this
     */
    public function save(): Record
    {
        //没有主键字段,则不进行保存
        if (!$this->_primaryKey) {
            return $this;
        }

        //获取要写入的数据
        $data = self::dataForWrite();

        //本表的主键字段
        $pk = $this->_primaryKey;

        //如果没有主键字段,插入数据
        if (!isset($data[$pk])) {
            $id = $this->_table->insert($data);
            $this->_data[$pk] = $id;
            $this->{$pk} = $id;
            return $this;
        }

        //以下是修改,主键的值
        $pkValue = $data[$pk];

        //与原始数据对比,挑出有变化的部分
        $data = array_diff_assoc($data, $this->_old);

        //如果有变化,只修改有变化的部分
        if (!empty($data)) {
            $this->_table->update($data, [$pk => $pkValue]);
        }

        //检查关联字段,逐个保存
        self::saveRelation();

        return $this;
    }

    /**
     * 根据当前数据作为条件,进行查询(一行)
     * @return $this
     */
    private function loadByData(): Record
    {
        $this->_loaded = true;
        $this->_data = $this->_table->row('*', self::dataForWrite())->toArray();
        return $this;
    }

    /**
     * 根据主键进行查询(一行)
     * @param $pk mixed
     * @return $this
     */
    private function loadByPk($pk): Record
    {
        $this->_loaded = true;
        $this->_data = $this->_table->row('*', [$this->_primaryKey => $pk])->toArray();
        return $this;
    }

    /**
     * 根据条件进行查询
     * @param array $where 查询条件
     * @return $this
     */
    private function loadByWhere(array $where): Record
    {
        $this->_loaded = true;
        $this->_data = $this->_table->row('*', $where)->toArray();
        return $this;
    }

    /**
     * 用于保存原始数据
     * @var array
     */
    private $_old;

    /**
     * 从库中读取数据
     * @param  $pk mixed 可以指定主键,也可以是条件
     * @return $this
     */
    public function load($pk = null): Record
    {
        if (is_array($pk)) {
            $this->loadByWhere($pk);
        } elseif ($pk) {
            $this->loadByPk($pk);
        } elseif ($this->_data) {
            $this->loadByData();
        } else {
            trigger_error('加载行记录对象时参数错误:' . json($pk), E_USER_ERROR);
        }

        //保存原始数据
        $this->_old = $this->_data;

        //执行读取后的钩子
        if ($this->_data) {
            $this->_data = $this->afterLoad($this->_data);
        }

        //将原始值赋值给属性
        if ($this->_data) {
            foreach ($this->_data as $k => $v) {
                $this->$k = $v;
            }
        }
        return $this;
    }

    /**
     * 强行设置本行数据已经从数据库中载入(用来从SROW构造Record)
     * @return $this
     */
    public function setLoaded(): Record
    {
        $this->_loaded = true;
        return $this;
    }

    /**
     * 实例化一个具体Record类
     * @param $className string 子类名
     * @param array $data
     * @return Record 具体会返回一个子类对象
     */
    public static function instanceRecord(string $className, array $data)
    {
        return new $className($data);
    }

    /**
     * 将Record转化为Row对象
     * @return Row
     */
    public function toRow(): Row
    {
        return new Row($this->tableName(), static::all());
    }

    /**
     * 转换成数组
     * @return array
     */
    public function toArray(): array
    {
        return $this->all();
    }

    /**
     * 数据是否为空,没找到相应的行
     * @return bool
     */
    public function isEmpty(): bool
    {
        return $this->toRow()->isEmpty();
    }

    /**
     * 为var_dump准备的
     */
    public function __debugInfo(): array
    {
        return $this->all();
    }

    /**
     * 读取数据后的钩子,子类可以重写
     * @param $data array 从数据库中读取的原始数据
     * @return array 处理后的数据
     */
    protected function afterLoad(array $data): array
    {
        return $data;
    }

    /**
     * 写入数据前的钩子,子类可以重写
     * @param $data array 内存中的数据
     * @return array 处理后写入数据库的数据
     */
    protected function beforeSave(array $data): array
    {
        return $data;
    }

    /**
     * 查看关联子对象,进行递归保存
     */
    private function saveRelation(): void
    {
        //逐个查看关联对象
        foreach (['_hasOne', '_hasMany', '_belongsTo', '_belongsToMany'] as $relation) {
            if (isset($this->$relation)) {
                foreach ($this->$relation as $r) {
                    $name = $r['name'];
                    if (isset($this->$name)) {
                        $this->$name->save();
                    }
                }
            }
        }
    }

    /**
     * 当读取一个不存在的属性时,检查是否是关联对象(懒加载)
     * @param $name string 属性名
     * @return Record|ResultSet 可能是一行或一个结果集
     */
    public function __get(string $name)
    {
        //逐个查看关联对象
        foreach (['_hasOne', '_hasMany', '_belongsTo', '_belongsToMany'] as $relation) {
            if (isset($this->$relation)) {
                foreach ($this->$relation as $r) {
                    if ($r['name'] == $name) {
                        return $this->$name = $this->$relation($r);
                    }
                }
            }
        }

        trigger_error('字段不存在:' . $name, E_USER_ERROR);
        return null;
    }

    /**
     * 获取一对一子对象的值
     * @param array $config 配置信息
     * @return Record 结果行对象
     */
    private function _hasOne(array $config): Record
    {
        //配置信息
        $tableClass = $config['table'];
        $primaryKey = $config['primaryKey'];
        $foreignKey = $config['foreignKey'];
        $orderBy = isset($config['orderBy']) ? $config['orderBy'] : null;

        //取主键的值
        $primaryValue = $this->$primaryKey;

        /**
         * 子对象表
         * @var $tableInstance Table
         */
        $tableInstance = $tableClass::instance();

        //在子对象表中查询关联记录
        $row = $tableInstance->row('*', [$foreignKey => $primaryValue], $orderBy);

        //转换成具体行对象
        $recordClass = 'R' . substr($tableClass, 1);
        return new $recordClass($row->toArray());
    }

    /**
     * 获取一对多子对象的值
     * @param array $config 配置信息
     * @return ResultSet 结果集对象
     */
    private function _hasMany(array $config): ResultSet
    {
        //配置信息
        $tableClass = $config['table'];
        $primaryKey = $config['primaryKey'];
        $foreignKey = $config['foreignKey'];

        //取主键的值
        $primaryValue = $this->$primaryKey;

        /**
         * 子对象表
         * @var $tableInstance Table
         */
        $tableInstance = $tableClass::instance();

        //在子对象表中查询关联记录
        return ResultSet::instance($tableInstance->select('*', [$foreignKey => $primaryValue]))
            ->_setType('hasMany', $config);
    }

    /**
     * 获取多对一子对象的值
     * @param array $config 配置信息
     * @return Record 结果行对象
     */
    private function _belongsTo(array $config): Record
    {
        //配置信息
        $tableClass = $config['table'];
        $primaryKey = $config['primaryKey'];//父表主键
        $foreignKey = $config['foreignKey']; //本表外键

        //取本表外键的值
        $foreignValue = $this->$foreignKey;

        /**
         * 父对象表
         * @var $tableInstance Table
         */
        $tableInstance = $tableClass::instance();

        //在父对象表中查询关联记录
        $row = $tableInstance->row('*', [$primaryKey => $foreignValue]);

        //返回父表行记录对象
        $recordClass = 'R' . substr($tableClass, 1);
        return new $recordClass($row->toArray());
    }

    /**
     * 获取一对多子对象的值
     * @param array $config 配置信息
     * @return ResultSet 结果集对象
     */
    private function _belongsToMany(array $config): ResultSet
    {
        //配置信息
        $targetClass = $config['table'];  //对方表
        $middleClass = $config['middle'];  //中间表
        $selfPrimaryKey = $config['selfPrimaryKey'];//本表主键
        $targetPrimaryKey = $config['targetPrimaryKey'];//对方 表主键
        $selfForeignKey = $config['selfForeignKey'];//中间表中与本表关联的键
        $targetForeignKey = $config['targetForeignKey'];//中间表中与对方 表关联的键

        //取本表主键的值
        $primaryValue = $this->$selfPrimaryKey;
        $config['selfPrimaryValue'] = $primaryValue;

        /**
         * 中间对象表
         * @var $middleInstance Table
         */
        $middleInstance = $middleClass::instance();

        //取中间表里,对方表外键的值
        $foreignValues = $middleInstance->col($targetForeignKey, [$selfForeignKey => $primaryValue]);

        /**
         * 对方表对象
         * @var $targetInstance Table
         */
        $targetInstance = $targetClass::instance();

        //在对方对象表中查询关联记录
        return ResultSet::instance($targetInstance->select('*', [$targetPrimaryKey => $foreignValues]))
            ->_setType('belongsToMany', $config);
    }
}