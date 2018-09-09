# table
对数据库请求进行封装

#此对象禁止实例化,使用单例模式
instance() :SMysql

#数据库连接对象
connect(string $alias, string $mode = 'write'): PDO

#获取数据库连接配置
getConfig(): array

#连接指定数据库
connectDatabase(array $connectInfo): PDO

#对某个表进行行锁定(默认写锁)
lock(string $tableName, string $level): void

#解除表锁定
unlock(string $tableName): void

#判断是否是字段名
isField(string $str): int

#加上mysql字段定界符 ``
markField(string $str): string

#把值进行处理,并加上定界符 
markValue($value): string

#将数组中每一个值,并加上定界符
markValueArray(array $values): array

#对字段列表进行标准化
formatFields($fields): array

#标准化字段列表,可以是各种输入格式
createFields($fields = null): string
