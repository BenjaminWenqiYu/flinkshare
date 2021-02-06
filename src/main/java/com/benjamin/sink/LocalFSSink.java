package com.benjamin.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2020-04-01
 * Time: 15:37
 * Description:
 */
public class LocalFSSink extends RichSinkFunction<String> {
    private final Logger LOG = LoggerFactory.getLogger(LocalFSSink.class);

    private String dirName;
    private boolean overwrite = false;
    private BufferedWriter writer;

    public LocalFSSink(String baseDir, boolean overwrite) {
        this.dirName = baseDir;
        this.overwrite = overwrite;
    }

    public LocalFSSink(String baseDir) {
        this.dirName = baseDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        File file = Paths.get(this.dirName, String.valueOf(taskId)).toFile();
        if (!overwrite && file.isFile() && file.exists()) {
            throw new Exception(file + " already exists!");
        } else {
            this.writer = new BufferedWriter(new FileWriter(file));
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (this.writer != null) {
            writer.write(value + System.getProperty("line.separator"));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
        }
    }

}
