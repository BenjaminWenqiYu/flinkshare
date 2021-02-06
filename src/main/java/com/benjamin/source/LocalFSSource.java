package com.benjamin.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2020-04-02
 * Time: 13:30
 * Description:
 * 自定义本地文件读取方法
 */
public class LocalFSSource extends RichParallelSourceFunction<String> {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private boolean running = true;
    private String dirName;
    private BufferedReader reader;

    ExecutorService executorService = null;

    public LocalFSSource(String baseDirName) {
        this.dirName = baseDirName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskId = this.getRuntimeContext().getIndexOfThisSubtask();
        File filePath = Paths.get(dirName, String.valueOf(taskId)).toFile();
        if (filePath.exists()) {
            LOG.info(filePath + " exists, ready to open");
            this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        } else {
            this.running = false;
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String line;
        while (running && (line = this.reader.readLine()) != null) {
            sourceContext.collect(line);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        if (this.reader != null) {
            try {
                this.reader.close();
            } catch (IOException e) {
                LOG.error("Error when close file handler, ", e);
            }
        }
    }
}
