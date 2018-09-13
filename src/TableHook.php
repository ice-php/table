<?php
declare(strict_types=1);

namespace icePHP;

/**
 * 为STableCache提供钩子相关方法
 * @author Administrator
 *
 */
trait TableHook
{
    // 七种方法,前后共14个钩子位置
    private $hooksBeforeInsert = [], $hooksAfterInsert = [], $hooksBeforeUpdate = [], $hooksAfterUpdate = [], $hooksBeforeDelete = [], $hooksAfterDelete = [], $hooksBeforeCrease = [], $hooksAfterCrease = [], $hooksBeforeExecute = [], $hooksAfterExecute = [], $hooksBeforeQuery = [], $hooksAfterQuery = [], $hooksBeforeSelect = [], $hooksAfterSelect = [];

    /**
     * 注册一个钩子
     *
     * @param string $property 操作 hooksBefore/hooksAfter . Insert/Update/Delete/Crease/Execute/Query/Select
     * @param mixed $callback 注册方法参数
     * @return Table 确认是Table而不是TableHook
     */
    private function reg($property, callable $callback): Table
    {
        array_unshift($this->$property, $callback);
        return $this;
    }

    /**
     * 注册 Insert的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeInsert(callable $callback): Table
    {
        return self::reg('hooksBeforeInsert', $callback);
    }

    /**
     * 注册Insert的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterInsert(callable $callback): Table
    {
        return self::reg('hooksAfterInsert', $callback);
    }

    /**
     * 注册Update的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeUpdate(callable $callback): Table
    {
        return self::reg('hooksBeforeUpdate', $callback);
    }

    /**
     * 注册Update的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterUpdate(callable $callback): Table
    {
        return self::reg('hooksAfterUpdate', $callback);
    }

    /**
     * 注册Delete的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeDelete(callable $callback): Table
    {
        return self::reg('hooksBeforeDelete', $callback);
    }

    /**
     * 注册Delete的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterDelete(callable $callback): Table
    {
        return self::reg('hooksAfterDelete', $callback);
    }

    /**
     * 注册Crease的前置 钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeCrease(callable $callback): Table
    {
        return self::reg('hooksBeforeCrease', $callback);
    }

    /**
     * 注册Crease的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterCrease(callable $callback): Table
    {
        return self::reg('hooksAfterCrease', $callback);
    }

    /**
     * 注册Execute的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeExecute(callable $callback): Table
    {
        return self::reg('hooksBeforeExecute', $callback);
    }

    /**
     * 注册Execute的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterExecute(callable $callback): Table
    {
        return self::reg('hooksAfterExecute', $callback);
    }

    /**
     * 注册Query的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeQuery(callable $callback): Table
    {
        return self::reg('hooksBeforeQuery', $callback);
    }

    /**
     * 注册Query的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterQuery(callable $callback): Table
    {
        return self::reg('hooksAfterQuery', $callback);
    }

    /**
     * 注册Select的前置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookBeforeSelect(callable $callback): Table
    {
        return self::reg('hooksBeforeSelect', $callback);
    }

    /**
     * 注册Select的后置钩子
     * @param $callback callable 回调方法
     * @return Table
     */
    public function hookAfterSelect(callable $callback): Table
    {
        return self::reg('hooksAfterSelect', $callback);
    }

    /**
     * 执行前置钩子
     * param $op string 指定操作 Insert/Update/Delete/Crease/Execute/Query/Select
     * param array ...$args 参数表
     * @return array|string 修改后的参数表或中止标识
     */
    protected function before()
    {
        $args = func_get_args();
        $op = array_shift($args);

        $property = 'hooksBefore' . $op;

        // 如果存在前置钩子
        foreach ($this->$property as $func) {
            // 执行前置 检查,
            $ret = call_user_func_array($func, $args);

            //检查失败则不再继续
            if ($ret === Table::INTERRUPT) {
                return $ret;
            }
            $args = $ret;
        }

        // 所有检查通过
        return $args;
    }

    /**
     * 执行后置钩子
     * param $op string 操作
     * param $params array 操作结果(有时,带一个前导操作符)
     * @return mixed 处理过的操作结果
     */
    protected function after(string $op, ...$params)
    {

        $property = 'hooksAfter' . $op;

        // 逐个执行
        foreach ($this->$property as $func) {
            // 对查询结果进行转换
            $params = call_user_func($func, $params);
        }
        // 返回转换结果
        return array_pop($params);
    }
}