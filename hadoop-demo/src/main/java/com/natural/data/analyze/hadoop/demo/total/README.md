

### 表结构

```
0    无意义
1    无意义
2    无意义
3    无意义
4    时间     2010-02  ，只取前四位
5    金额数

```

### 在 active 的namenode上面创建文件夹
```
hadoop fs -mkdir /tmp
hadoop fs -mkdir /tmp/input
hadoop fs -mkdir /tmp/output

hadoop fs -put ./data1.csv /tmp/input

```

### 把当前项目 build 成jar 包，放在server 中运行

```
hadoop jar hadoop-demo-1.0.0.jar /tmp/input/data1.csv /tmp/output/data1.out
```
### 查看结果
```
hadoop fs -cat /tmp/output/data1.out/part-r-00000
```