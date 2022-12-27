## hbase-hfile使用说明
### 功能
读取HBase源表的HFile文件内容，通过Bulkload方式加载并写入HBase目标表
### 使用方法
    # 授权
    chmod u+x hbase-hfile
    # 执行脚本，执行用户需要有HBase表的读写权限
    ./hbase-hfile [src_namespace] [src_table] [dst_namespace] [dst_table]
	# 编辑公共参数,一般只修改一次即可
	vim hbase-hfile
	[defaultFs] [src_hbaseRootDir] [dst_zkQuorum] [dst_hbaseRootDir] 替换为真实参数
    # 执行脚本示例
    ./hbase-hfile default:panTag test:panTag


### 公共参数说明
|  参数名称   | 参数说明  | 参数示例
|  ----  | ----  | ---- |
| defaultFs  | hdfs集群地址 | hdfs://xxx:8020|
| src_hbaseRootDir  | 源HBase集群hbase-site.xml中hbase.rootdir属性值 |缺省/hbase|
| dst_zkQuorum  | 目标HBase集群zookeeper地址 |x.x.x.x:2181|
| dst_hbaseRootDir  | 目标HBase集群hbase-site.xml中hbase.rootdir属性值 |缺省/hbase|
### 可选参数说明
|  参数名称   | 参数说明  | 参数示例
|  ----  | ----  | ---- |
| src_namespace:src_table  | HFile所属HBase源表namespace:HBase源表名称 |缺省namespace为default|
| dst_namespace:dst_table  | HBase目标表namespace:HBase目标表名称 |缺省namespace为default|

