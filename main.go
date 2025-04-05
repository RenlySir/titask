package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 配置文件结构
type Config struct {
	DB    DBConfig              `toml:"db"`
	Tasks map[string]TaskConfig `toml:"task"`
}

var (
	configFile string
)

type DBConfig struct {
	Host            string        `toml:"host"`
	Port            int           `toml:"port"`
	User            string        `toml:"user"`
	Passwd          string        `toml:"passwd"`
	DBName          string        `toml:"dbname"`
	Params          string        `toml:"params"`
	MaxOpenConns    int           `toml:"maxOpenConns"`
	MaxIdleConns    int           `toml:"maxIdleConns"`
	ConnMaxLifetime time.Duration `toml:"connMaxLifetime"`
}
type TaskConfig struct {
	Thread         int    `toml:"thread"`
	SQL            string `toml:"sql"`
	TargetTable    string `toml:"targetTable"`
	BatchSQL       string `toml:"batchSQL"`
	WaitForTiflash int    `toml:"waitForTiflash"`
}

// 分区信息结构
type Partition struct {
	Name  string
	Value string
}

// 目标表解析结果
type TargetTable struct {
	DBName    string
	TableName string
}

func init() {
	flag.StringVar(&configFile, "config", "ctask.toml", "配置文件路径")
}

func main() {
	flag.Parse()

	// 加载配置文件
	var config Config
	if _, err := toml.DecodeFile(configFile, &config); err != nil {
		log.Fatal("配置文件解析失败:", err)
	}
	// 构建DSN（强制使用文本协议）
	dsn := buildDSN(config.DB)

	// 初始化数据库连接池
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("数据库连接失败:", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(config.DB.MaxOpenConns)
	db.SetMaxIdleConns(config.DB.MaxIdleConns)
	db.SetConnMaxLifetime(config.DB.ConnMaxLifetime)

	// 处理所有任务
	//var wg sync.WaitGroup
	//for taskName, taskConfig := range config.Tasks {
	//	wg.Add(1)
	//	go func(name string, cfg TaskConfig) {
	//		defer wg.Done()
	//		if err := processTask(db, cfg); err != nil {
	//			log.Printf("任务%s执行失败: %v", name, err)
	//		}
	//	}(taskName, taskConfig)
	//}
	//wg.Wait()

	// 获取排序后的任务列表
	sortedTasks, err := sortTasks(config.Tasks)
	if err != nil {
		log.Fatal("任务排序失败:", err)
	}

	for _, taskName := range sortedTasks {
		taskCfg := config.Tasks[taskName]
		log.Printf("======== 开始执行任务 %s ========", taskName)
		// 执行当前任务
		if err := processTask(db, taskCfg); err != nil {
			log.Fatalf("任务 %s 执行失败: %v", taskName, err)
		}
		if taskCfg.WaitForTiflash == 1 {
			target, err := parseTargetTable(taskCfg.TargetTable)
			if err != nil {
				log.Fatalf("目标表解析失败: %v", err)
			}
			log.Printf("等待 %s.%s 的TiFlash副本就绪...", target.DBName, target.TableName)
			if err := waitForTiflashReady(db, target.DBName, target.TableName); err != nil {
				log.Fatal(err)
			}
		}
	}

}

func waitForTiflashReady(db *sql.DB, dbName string, tableName string) error {
	const (
		maxRetry = 6000             // 最大重试次数
		interval = 20 * time.Second // 检查间隔
	)
	fmt.Printf("表%s.%s开启tiflash副本", dbName, tableName)
	_, err := db.Exec(fmt.Sprintf("alter table %s.%s set tiflash replica 2", dbName, tableName))
	if err != nil {
		fmt.Printf("开启列存副本失败： %s \n", err)
		return err
	}

	query := `SELECT available FROM information_schema.tiflash_replica 
             WHERE table_schema = ? AND table_name = ?`

	for i := 0; i < maxRetry; i++ {
		var available int
		err := db.QueryRow(query, dbName, tableName).Scan(&available)
		switch {
		case err == sql.ErrNoRows:
			return fmt.Errorf("找不到TiFlash副本信息")
		case err != nil:
			return fmt.Errorf("查询失败: %v", err)
		case available == 1:
			log.Printf("TiFlash副本已就绪（第%d次检查）", i+1)
			return nil
		default:
			log.Printf("第%d次检查，当前状态: %d（等待中...）", i+1, available)
			time.Sleep(interval)
		}
	}
	return fmt.Errorf("等待TiFlash副本超时（最大等待时间：%v）", maxRetry*interval)
}

func sortTasks(tasks map[string]TaskConfig) ([]string, error) {
	type taskInfo struct {
		name  string
		index int
	}

	// 提取任务序号
	var taskList []taskInfo
	for name := range tasks {
		// 使用正则提取数字部分
		re := regexp.MustCompile(`task(\d+)`)
		matches := re.FindStringSubmatch(name)
		if len(matches) < 2 {
			return nil, fmt.Errorf("无效的任务名称格式: %s", name)
		}

		index, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("任务序号解析失败: %s", name)
		}

		taskList = append(taskList, taskInfo{name: name, index: index})
	}

	// 按序号排序
	sort.Slice(taskList, func(i, j int) bool {
		return taskList[i].index < taskList[j].index
	})

	// 生成排序后的名称列表
	var sortedNames []string
	for _, t := range taskList {
		sortedNames = append(sortedNames, t.name)
	}
	return sortedNames, nil
}

func buildDSN(dbCfg DBConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		dbCfg.User,
		dbCfg.Passwd,
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.DBName,
		dbCfg.Params)
}

// 数据库操作函数
func parseTargetTable(target string) (TargetTable, error) {
	parts := strings.Split(target, ".")
	if len(parts) != 2 {
		return TargetTable{}, fmt.Errorf("无效的targetTable格式: %s", target)
	}
	return TargetTable{
		DBName:    parts[0],
		TableName: parts[1],
	}, nil
}

func getPartitions(db *sql.DB, cfg TaskConfig) ([]Partition, error) {
	rows, err := db.Query(cfg.BatchSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []Partition
	for rows.Next() {
		var p Partition
		if err := rows.Scan(&p.Name, &p.Value); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

// 处理数据操作（插入+交换）
func processDataOperations(db *sql.DB, cfg TaskConfig, target TargetTable, partitions []Partition) error {
	ch := make(chan Partition, len(partitions))
	var wg sync.WaitGroup
	var errs []error
	var mu sync.Mutex

	// 启动worker
	for i := 0; i < cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				if err := processPartitionData(db, cfg, target, p); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("分区%s: %v", p.Name, err))
					mu.Unlock()
				}
			}
		}()
	}

	// 分发任务
	for _, p := range partitions {
		ch <- p
	}
	close(ch)
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("发现%d个错误: %v", len(errs), errs)
	}
	return nil
}

// 统一删除所有临时表
func dropAllTempTables(db *sql.DB, dbName string, tables []string) {
	var wg sync.WaitGroup
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			dropTempTable(db, dbName, t)
		}(table)
	}
	wg.Wait()
}

// 处理单个分区数据操作
func processPartitionData(db *sql.DB, cfg TaskConfig, target TargetTable, p Partition) error {
	tempTable := fmt.Sprintf("%s_%s", target.TableName, p.Name)
	fullTempTable := fmt.Sprintf("`%s`.`%s`", target.DBName, tempTable)

	//fmt.Printf("执行数据插入------")
	// 执行数据插入
	if err := executeInsert(db, cfg.SQL, fullTempTable, p.Value); err != nil {
		return fmt.Errorf("数据插入失败: %v", err)
	}
	//fmt.Printf("执行分区交换------")
	// 执行分区交换
	if err := exchangePartition(db, target, tempTable, p.Name); err != nil {
		return fmt.Errorf("分区交换失败: %v", err)
	}

	return nil
}

func dropTempTable(db *sql.DB, dbName, table string) {
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, table))
	//fmt.Printf("删除表语句: DROP TABLE IF EXISTS %s.%s", dbName, table)
}

// 处理单个任务
func processTask(db *sql.DB, cfg TaskConfig) error {
	target, err := parseTargetTable(cfg.TargetTable)
	if err != nil {
		return fmt.Errorf("目标表解析失败: %v", err)
	}

	partitions, err := getPartitions(db, cfg)
	if err != nil {
		return fmt.Errorf("获取分区信息失败: %v", err)
	}

	// 预创建所有临时表
	tempTables, err := createAllTempTables(db, target, partitions)

	if err != nil {
		return err
	}
	log.Printf("成功创建%d个临时表", len(tempTables))

	// 处理数据操作
	if err := processDataOperations(db, cfg, target, partitions); err != nil {
		return fmt.Errorf("数据操作失败: %v /n", err)
	}

	defer func() {
		log.Printf("开始清理%d个临时表 /n", len(tempTables))
		dropAllTempTables(db, target.DBName, tempTables)
	}()

	return nil
}

// 预创建所有临时表（增强错误处理）
func createAllTempTables(db *sql.DB, target TargetTable, partitions []Partition) ([]string, error) {
	var tempTables []string
	var mu sync.Mutex
	errCh := make(chan error, len(partitions))
	var wg sync.WaitGroup

	for _, p := range partitions {
		wg.Add(1)
		go func(p Partition) {
			defer wg.Done()
			tempTable := fmt.Sprintf("%s_%s", target.TableName, p.Name)

			if err := createTempTable(db, target, tempTable); err != nil {
				errCh <- fmt.Errorf("临时分区表%s创建失败: %v", p.Name, err)
				return
			}

			mu.Lock()
			tempTables = append(tempTables, tempTable)
			mu.Unlock()
		}(p)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {

		dropAllTempTables(db, target.DBName, tempTables)
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}
		return nil, fmt.Errorf("创建临时表失败: %v", errs)
	}

	return tempTables, nil
}

// 修改后的创建临时表函数
func createTempTable(db *sql.DB, target TargetTable, tempTable string) error {
	var createSQL, tableName string
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", target.DBName, target.TableName)
	if err := db.QueryRow(query).Scan(&tableName, &createSQL); err != nil {
		return fmt.Errorf("获取表结构失败: %v", err)
	}

	// 增强分区定义移除逻辑
	createSQL = regexp.MustCompile(`(?is)/*\s*PARTITION BY.*$`).ReplaceAllString(createSQL, "")

	// 替换表名时排除注释内容
	re := regexp.MustCompile(fmt.Sprintf("`%s`", regexp.QuoteMeta(target.TableName)))
	createSQL = re.ReplaceAllString(createSQL,
		fmt.Sprintf("`%s`.`%s`", target.DBName, tempTable))
	//fmt.Printf("创建临时表 %s", createSQL)
	createSQL = regexp.MustCompile(`(?i)^CREATE\s+(TEMPORARY\s+)?TABLE`).
		ReplaceAllString(createSQL, "CREATE ${1}TABLE IF NOT EXISTS")
	// 执行前验证分区定义已移除
	if strings.Contains(strings.ToUpper(createSQL), "PARTITION BY") {
		return fmt.Errorf("分区定义移除失败，生成的建表语句仍包含分区信息")
	}

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("执行建表失败: %v\nSQL: %s", err, createSQL)
	}
	return nil
}

// 数据插入（增强SQL处理）
func executeInsert(db *sql.DB, baseSQL, tempTable string, value interface{}) error {
	// 增强正则表达式匹配
	re := regexp.MustCompile(`(?i)(IMPORT\s+INTO\s+)([\w\.]+\b|` + "`[^`]+`)")
	if !re.MatchString(baseSQL) {
		return fmt.Errorf("无效的INSERT语句格式")
	}

	newSQL := re.ReplaceAllString(baseSQL, fmt.Sprintf("${1}%s", tempTable))

	// 替换所有问号为value
	if strings.Contains(newSQL, "?") {
		newSQL = strings.Replace(newSQL, "?", fmt.Sprintf("'%s'", value), -1)
	}

	if _, err := db.Exec(newSQL); err != nil {
		return fmt.Errorf("执行插入失败: %v\nSQL: %s\n参数: %v", err, newSQL, value)
	}
	return nil
}

// 分区交换操作（增加存在性检查）
func exchangePartition(db *sql.DB, target TargetTable, tempTable, partition string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	mainTable := fmt.Sprintf("`%s`.`%s`", target.DBName, target.TableName)
	fullTempTable := fmt.Sprintf("`%s`.`%s`", target.DBName, tempTable)

	// 检查分区是否存在
	var exists bool
	err = tx.QueryRow(
		`SELECT COUNT(1) 
		FROM information_schema.PARTITIONS 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_NAME = ? 
		AND PARTITION_NAME = ?`,
		target.DBName, target.TableName, partition,
	).Scan(&exists)
	if err != nil || !exists {
		return fmt.Errorf("分区%s不存在: %v", partition, err)
	}

	// 执行分区维护
	if _, err := tx.Exec(
		fmt.Sprintf("ALTER TABLE %s TRUNCATE PARTITION %s",
			mainTable, partition)); err != nil {
		return err
	}
	fmt.Printf(" 交换分区 ALTER TABLE %s TRUNCATE PARTITION %s \n ", mainTable, partition)

	//fmt.Printf(" 交换分区失败===== SQL:    ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s",
	//	mainTable, partition, fullTempTable)
	if _, err := tx.Exec(
		fmt.Sprintf("ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s",
			mainTable, partition, fullTempTable)); err != nil {
		return fmt.Errorf("交换分区失败: %v", err)
	}

	return tx.Commit()
}
