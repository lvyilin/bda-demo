package com.github.lvyilin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseUtils {

    public static Connection getConnection() {
        try {
            return ConnectionFactory.createConnection(getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Configuration getConfiguration() {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path(HBaseConsts.HBASE_CONF_PATH, "hbase-site.xml"));
        config.addResource(new Path(HBaseConsts.HADOOP_CONF_PATH, "core-site.xml"));
        return config;
    }

    public static void main(String[] args) {
        Connection connection = getConnection();
    }

}
