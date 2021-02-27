package com.story.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HBaseDemo {
    Configuration conf = null;
    Connection conn = null;
    Admin admin = null;
    TableName tableName = TableName.valueOf("shop");
    //TableName tableName = TableName.valueOf("phonerecord");
    Table table = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zknode02,zknode03,zknode04");

        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();

        table = conn.getTable(tableName);
    }

    @Test
    public void create() throws IOException {
        //定义表描述
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        //定义列族描述
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
        //添加列族信息给表
        builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        //创建表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName); //禁用表
            admin.deleteTable(tableName); //删除表
        }
        admin.createTable(builder.build());
    }

    @Test
    public void insert() {
        Put data = new Put(Bytes.toBytes("1"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("iphone"), Bytes.toBytes("6000"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("huawei"), Bytes.toBytes("5888"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("vivo"), Bytes.toBytes("4500"));
        try {
            table.put(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insert1() {
        Put data = new Put(Bytes.toBytes("2"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("iphone"), Bytes.toBytes("5000"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("huawei"), Bytes.toBytes("3000"));
        data.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("vivo"), Bytes.toBytes("2500"));
        try {
            table.put(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get() {
        Get get = new Get(Bytes.toBytes("1"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("iphone"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("huawei"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("vivo"));

        try {
            Result result = table.get(get);
            Cell c1 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("iphone"));
            Cell c2 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("huawei"));
            Cell c3 = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("vivo"));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c1)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c2)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c3)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getScan() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            Cell c1 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("iphone"));
            Cell c2 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("huawei"));
            Cell c3 = rs.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("vivo"));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c1)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c2)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(c3)));
        }
    }

    /**
     *
     * 查询某个用户3月份的通话记录
     * phonerecord表
     */
    @Test
    public void ScanByCondition() throws Exception {
        Scan scan = new Scan();
        String startRow = "15886593944_" + (Long.MAX_VALUE - format.parse("20210331000000").getTime());
        String stopRow = "15886593944_" + (Long.MAX_VALUE - format.parse("20210301000000").getTime());
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            Cell receNumber = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("receNumber"));
            Cell len = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("len"));
            Cell date = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
            Cell type = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
            System.out.print(Bytes.toString(CellUtil.cloneValue(receNumber)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(len)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(date)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(type)));
            System.out.println();
        }
    }

    /**
     * 查询用户的主叫记录 type=1
     * phonerecord表
     * */
    @Test
    public void getRecordByType() throws Exception {
        Scan scan = new Scan();
        //创建过滤器集合
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //创建过滤器
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cf"),Bytes.toBytes("type"),
                CompareOperator.EQUAL,Bytes.toBytes("1"));
        filters.addFilter(filter);
        //前缀过滤器
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("15886593944"));
        filters.addFilter(prefixFilter);

        scan.setFilter(filters);
        ResultScanner results = table.getScanner(scan);
        for (Result r : results) {
            Cell receNumber = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("receNumber"));
            Cell len = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("len"));
            Cell date = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("date"));
            Cell type = r.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("type"));
            System.out.print(Bytes.toString(CellUtil.cloneValue(receNumber)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(len)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(date)));
            System.out.print("---"+Bytes.toString(CellUtil.cloneValue(type)));
            System.out.println();
        }

    }

    /**
     * phonerecord
     * */
    @Test
    public void insertLotsOfDatas() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0;i<10;i++){
            String phoneNum = getNumber("158");
            for (int j = 0; j < 10000; j++) {
                String receNumber = getNumber("130");
                String len = String.valueOf(random.nextInt(100));
                String date = getDate("2021");
                String type = String.valueOf(random.nextInt(2));
                //iphone+ Long.MAX_VALUE减去时间戳，实现倒叙
                String rowKey = phoneNum+"_"+(Long.MAX_VALUE-format.parse(date).getTime());

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("receNumber"),Bytes.toBytes(receNumber));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("len"),Bytes.toBytes(len));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("date"),Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("type"),Bytes.toBytes(type));

                puts.add(put);
            }
        }
        table.put(puts);
    }

    private String getDate(String s) {
        return s.concat(String.format("%02d%02d%02d%02d%02d",
                random.nextInt(12)+1,
                random.nextInt(28),
                random.nextInt(24),
                random.nextInt(60),
                random.nextInt(60)
                ));
    }

    Random random = new Random();
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    public String getNumber(String str) {
        return str.concat(String.format("%08d", random.nextInt(99999999)));
    }

    /**
     * shop表
     * */
    @Test
    public void delete() throws Exception {
        Delete delete = new Delete(Bytes.toBytes("1"));
        delete.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("huawei"));
        table.delete(delete);

    }

    @After
    public void destory() {
        try {
            table.close();
            admin.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
