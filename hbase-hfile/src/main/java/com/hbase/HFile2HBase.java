package com.hbase;


import com.hbase.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HFile2HBase {
    private static final Logger LOG = LoggerFactory.getLogger(HFile2HBase.class);

    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("<Usage> Please input <defaultFs> <src_hbaseRootDir> <dst_zkQuorum> <dst_hbaseRootDir> <src_namespace:src_table> <dst_namespace:dst_table>. </Usage>");
            return;
        }
        String defaultFs = args[0].trim();
        String srcHBaseRootDir = args[1].trim();
        String dstZkQuorum = args[2].trim();
        String dstHBaseRootDir = args[3].trim();
        String srcNamespace = args[4].split(":")[0].trim();
        String srcTable = args[4].split(":")[1].trim();
        String dstNamespace = args[5].split(":")[0].trim();
        String dstTable = args[5].split(":")[1].trim();
        FileSystem fs = HdfsUtil.getFs(defaultFs);
        String srcHfileDir = getPath(srcHBaseRootDir + "/data", srcNamespace, srcTable);
        if (!HdfsUtil.exist(fs, srcHfileDir)) {
            LOG.error(">>>>> hfile path does not exist.");
            return;
        }
        List<Path> list = getHfilePaths(fs, srcHfileDir);
        if (list.isEmpty()) {
            LOG.error(">>>>> hfile is empty.");
            return;
        }
        String tmpDir = getPath("/tmp/hfile", srcNamespace, srcTable);
        if (HdfsUtil.exist(fs, tmpDir)) {
            HdfsUtil.delete(fs, tmpDir);
        }
        list.forEach(e -> {
            String pathStr = e.toString();
            String cfPath = pathStr.substring(pathStr.lastIndexOf("/", pathStr.lastIndexOf("/") - 1));
            String srcPathStr = e.toUri().getPath();
            String dstPathStr = tmpDir + cfPath;
            LOG.info("src_file={},dst_file={}", srcPathStr, dstPathStr);
            HdfsUtil.copy(fs, e.toUri().getPath(), dstPathStr);
            HdfsUtil.setPermission(fs, dstPathStr);
        });
        String loadPath = defaultFs + tmpDir;
        doBulkLoad(dstZkQuorum, dstHBaseRootDir, loadPath, dstNamespace + ":" + dstTable);
        LOG.error(">>>>> HFile2HBase success! >>>>>");
    }

    private static void doBulkLoad(String zkQuorum, String hbaseRootDir, String loadPath, String table) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseRootDir);
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(conf);
            Admin admin = hConnection.getAdmin();
            Table hTable = hConnection.getTable(TableName.valueOf(table));
            RegionLocator regionLocator = hConnection.getRegionLocator(TableName.valueOf(table));
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(new Path(loadPath), admin, hTable, regionLocator);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (hConnection != null) {
                    hConnection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static String getPath(String prefix, String namespace, String table) {
        return String.format("%s/%s/%s", prefix, namespace, table);
    }

    private static List<Path> getHfilePaths(FileSystem fs, String rootDir) {
        List<Path> paths = HdfsUtil.getHdfsPaths(fs, rootDir);
        return paths.stream().filter(f -> f.getName().length() == 32).collect(Collectors.toList());
    }

}
