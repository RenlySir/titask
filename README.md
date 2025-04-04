manual

1、设计思路

1.1、涉及的所有基础表找到分区键，表结构均改造为分区表，且所有基础表的分区名与值相对应。如(p0,202202)(p1,202203)(p2,202204)

1.2、创建库task，表pv。将分区名与值相对应插入至task.pv中，如 insert into task.pv values('p0','202202'),('p1','202203'),('p2','202204');

3、配置文件说明

[db]

host = "113.44.138.199" 

port = 4000

user = "root"

passwd = ""

dbname = "test" # 建议填写查询的库名

maxOpenConns=25

maxIdleConns=25

connMaxLifetime=60000

params=""

[task.task1]
thread = 6

sql = "IMPORT INTO db1.t1 FROM SELECT * FROM t WHERE 费款所属期=?" # 使用import into 改造后的SQL模板

targetTable="db1.t1" # 需要写入的库表名称，创建临时表时使用

batchSQL="select * from task.pv" # 通过该sql查询partiton name、partition value，为上面的sql赋值以及中途创建临时表使用；

4、运行

./task1 -config ctask.toml 2>&1 | tee run.log

5、未来维护
随着日期推移，每个月需为6张基础表增加分区，如现在是202504，为每张表增加一个分区 alter table db1.tb1 add partition (partition p81 VALUES IN ('202505');

并维护task.pv表，执行insert into task.pv('p81','202505');










