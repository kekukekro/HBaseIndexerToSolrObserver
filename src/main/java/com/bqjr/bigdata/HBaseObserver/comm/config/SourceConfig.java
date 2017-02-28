package com.bqjr.bigdata.HBaseObserver.comm.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * Created by hp on 2017-02-16.
 */
public class SourceConfig {
    protected Config config;
    protected String configHome = "/etc/hbase/conf/";

    public Config getConfig() {
        return this.config;
    }

    /**
     * 设置配置文件，只写文件名即可。
     * <p>
     * 默认从configHome路径去读文件，同时如果resources下有同名文件也会读取   *
     * 注意：在前面的文件会覆盖后面的文件的相同key的配置值
     *
     * @param file
     */

    public void setConfigFiles(String file, String... args) {
        config = load(file);
        if (args.length > 0) {
            for (String f : args) {
                config = config.withFallback(load(f));
            }
        }
    }

    public Config load(String file) {
        String resouceFile = file;
        File configFile = new File(makePath(file));
        if (configFile.exists()) {
            return ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load(resouceFile));
        } else {
            return ConfigFactory.load(resouceFile);
        }
    }

    public String makePath(String filename) {
        String newDir = configHome.trim().replaceAll("\\\\", "/");
        if (newDir.endsWith("/")) {
            return configHome + filename;
        } else {
            return configHome + "/" + filename;
        }

    }
}
