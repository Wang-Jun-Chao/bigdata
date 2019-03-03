package wjc.bigdata.algorithm.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-15 16:02
 **/
public class PathUtils {
    public final static String HDFS_HOST = "hdfs://localhost:8020/";

    private final static String           OUTPUT_SUFFIX = "-yyyyMMdd-HHmmss";
    private final static SimpleDateFormat SDF           = new SimpleDateFormat(OUTPUT_SUFFIX);

    public static String inputPath(String path) {
        return HDFS_HOST + path;
    }

    public static String outputPath(String path) {
        return HDFS_HOST + path + SDF.format(new Date());
    }

    public static String outputPathWithoutHost(String path) {
        return path + SDF.format(new Date());
    }

    public static String workDir() {
        return PathUtils.class.getClassLoader().getResource("").getPath();
    }

    public static String workDir(String path) {
        return workDir() + path;
    }

    public static void main(String[] args) {
        System.out.println(workDir());
    }

    public static boolean removeWorkDir(String s) {
        try {
            FileUtils.deleteDirectory(new File(workDir(s)));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }
}
