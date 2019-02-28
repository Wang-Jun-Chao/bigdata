package wjc.bigdata.algorithm.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.OutputStream;

//

/**
 * This class provides convenient methods for accessing
 * some Input/Output methods.
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 */
public class InputOutputUtil {

    public static void close(LineReader reader) {
        if (reader == null) {
            return;
        }
        //
        try {
            reader.close();
        } catch (Exception ignore) {
        }
    }

    public static void close(OutputStream stream) {
        if (stream == null) {
            return;
        }
        //
        try {
            stream.close();
        } catch (Exception ignore) {
        }
    }

    public static void close(InputStream stream) {
        if (stream == null) {
            return;
        }
        //
        try {
            stream.close();
        } catch (Exception ignore) {
        }
    }

    public static void close(FSDataInputStream stream) {
        if (stream == null) {
            return;
        }
        //
        try {
            stream.close();
        } catch (Exception ignore) {
        }
    }

    public static void close(BufferedReader reader) {
        if (reader == null) {
            return;
        }
        //
        try {
            reader.close();
        } catch (Exception ignore) {
        }
    }

}
