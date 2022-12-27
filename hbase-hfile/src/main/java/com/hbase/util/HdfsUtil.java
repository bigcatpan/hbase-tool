package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

    public static FileSystem getFs(String defaultFs) {
        FileSystem fs = null;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFs);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(URI.create(defaultFs), conf);
        } catch (Exception e) {
            LOG.error("getFs failed", e);
        }
        return fs;
    }

    public static List<Path> getHdfsPaths(FileSystem fs, String rootDir) {
        List<Path> paths = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(rootDir), true);
            while (it.hasNext()) {
                LocatedFileStatus file = it.next();
                Path path = file.getPath();
                paths.add(path);
                LOG.debug("file name =" + path.getName());
            }
        } catch (Exception e) {
            LOG.error("getHFilePaths failed", e);
        }
        return paths;
    }

    public static void copy(FileSystem fs, String srcPathString, String dstPathString) {
        try {
            FileUtil.copy(fs, new Path(srcPathString), fs, new Path(dstPathString), false, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean exist(FileSystem fs, String pathString) {
        try {
            return fs.exists(new Path(pathString));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void create(FileSystem fs, String pathString) {
        try {
            fs.create(new Path(pathString), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void delete(FileSystem fs, String pathString) {
        try {
            fs.delete(new Path(pathString), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setPermission(FileSystem fs, String pathString) {
        //set global read so RegionServer can move it
        try {
            fs.setPermission(new Path(pathString), FsPermission.valueOf("-rwxrwxrwx"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
