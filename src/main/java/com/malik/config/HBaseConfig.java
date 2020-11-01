package com.malik.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HBaseConfig {
    private final String outputTable;
    private final String hbaseMaster;
    private final String zkQuorum;

    public HBaseConfig(String outputTable, String hbaseMaster, String zkQuorum) {
        this.outputTable = outputTable;
        this.hbaseMaster = hbaseMaster;
        this.zkQuorum = zkQuorum;
    }

    /**
     * Set hbase config
     * @return key value object of config
     */
    private Configuration hbaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", hbaseMaster);
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.zookeeper.quorum", zkQuorum);
        configuration.set("hbase.client.keyvalue.maxsize", "0");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "100000");
        configuration.set("mapred.output.dir", "/tmp");
        configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");

        return configuration;
    }

    /**
     * Set hbase job
     * @return hbase job context
     * @throws IOException
     */
    public Job hbaseJob() throws IOException {
        Job job = Job.getInstance(hbaseConfiguration());
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
        job.setOutputFormatClass(TableOutputFormat.class);

        return job;
    }
}
