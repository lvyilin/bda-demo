package com.github.lvyilin;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConsumerExecutorService implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private KafkaStream stream;
    private static Connection connection = HBaseUtils.getConnection();
    private static TableName tableName = TableName.valueOf(HBaseConsts.TABLE_NAME);
    private static Table table;
    private static final byte[] cfInfoBytes = Bytes.toBytes("info");
    private static final byte[] cfTicketBytes = Bytes.toBytes("ticket");
    private static final byte[] cfErrBytes = Bytes.toBytes("err");
    private static final byte[] colDtBytes = Bytes.toBytes("dt");
    private static final byte[] colReqBytes = Bytes.toBytes("req");
    private static final byte[] colResBytes = Bytes.toBytes("res");
    private static final byte[] colSuccessBytes = Bytes.toBytes("success");
    private static final byte[] colMacBytes = Bytes.toBytes("mac");
    private static final byte[] colPcmBytes = Bytes.toBytes("pcm");
    private static final byte[] colAirportBytes = Bytes.toBytes("airport");
    private static final byte[] colAirlineBytes = Bytes.toBytes("airline");
    private static final byte[] colAgentBytes = Bytes.toBytes("agent");
    private static final byte[] colCountryBytes = Bytes.toBytes("country");
    private static final byte[] colErrnoBytes = Bytes.toBytes("error");
    private static final byte[] colTypeBytes = Bytes.toBytes("type");

    static {
        try {
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ConsumerExecutorService(KafkaStream stream) {
        this.stream = stream;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String message = new String(it.next().message());
            try {
                addRecords(message);
                logger.info("consume: " + message);
            } catch (IOException e) {
                logger.error("error" + message);
                e.printStackTrace();
            }

        }
    }

    private void addRecords(String row) throws IOException {
        String[] fieldSplit = row.split("\\|");
        assert fieldSplit.length == HBaseConsts.COLUMN_NUM;

        Put put = new Put(Bytes.toBytes(fieldSplit[0]));

        boolean success = fieldSplit[13].equals("1");
        put.addColumn(cfInfoBytes, colDtBytes, Bytes.toBytes(fieldSplit[1]));
        put.addColumn(cfInfoBytes, colReqBytes, Bytes.toBytes(fieldSplit[8]));
        put.addColumn(cfInfoBytes, colResBytes, Bytes.toBytes(fieldSplit[9]));
        put.addColumn(cfInfoBytes, colSuccessBytes, Bytes.toBytes(success ? "1" : "0"));
        put.addColumn(cfTicketBytes, colMacBytes, Bytes.toBytes(fieldSplit[2]));
        put.addColumn(cfTicketBytes, colPcmBytes, Bytes.toBytes(fieldSplit[3]));
        put.addColumn(cfTicketBytes, colAirportBytes, Bytes.toBytes(fieldSplit[4]));
        put.addColumn(cfTicketBytes, colAirlineBytes, Bytes.toBytes(fieldSplit[5]));
        put.addColumn(cfTicketBytes, colAgentBytes, Bytes.toBytes(fieldSplit[6]));
        put.addColumn(cfTicketBytes, colCountryBytes, Bytes.toBytes(fieldSplit[7]));
        if (!success) {
            put.addColumn(cfErrBytes, colErrnoBytes, Bytes.toBytes(fieldSplit[11]));
            put.addColumn(cfErrBytes, colTypeBytes, Bytes.toBytes(fieldSplit[12]));
        }

        table.put(put);

    }


}
