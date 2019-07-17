package com.briup.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class HbaseTest1 {
    private boolean flag = false;
    private static Admin admin = null;
    private static Connection connection = null;
    private static ExecutorService pool = Executors.newFixedThreadPool(10);
    private static Configuration conf;
    //数据源：封装了数据库的连接过程，并实现了连接池

    public void getConnection() throws Exception {
        //拿到配置对象
        conf = HBaseConfiguration.create();
        //
        conf.set("hbase.zookeeper.quorum", "192.168.239.211");//这里的端口在配置文件中有默认2181    192.168.239.211：2181

        connection = ConnectionFactory.createConnection(conf);
        System.out.println("连接成功!" + connection);
        //拿到admin对象
        admin = connection.getAdmin();
        System.out.println("拿到对象:admin");
        connection = admin.getConnection();
        System.out.println("已连接...");

    }

    //异步连接
    public void getConnection_asyn() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //
        conf.set("hbase.zookeeper.quorum", "192.168.239.211:2181");//这里的端口在配置文件中有默认2181    192.168.239.211：2181

        //回调对象
        CompletableFuture<AsyncConnection> cb = ConnectionFactory.createAsyncConnection(conf);
        //回调对象去获得连接对象
        AsyncConnection asyncConnection = cb.get(10000, TimeUnit.MILLISECONDS);
        System.out.println("连接成功!" + connection);
//        AsyncAdmin asyncAdmin = asyncConnection.getAdmin();

    }

    public void close() throws IOException {
        admin.close();
        connection.close();
    }

    public void createNS(String namespace) throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        //建造者模式
        NamespaceDescriptor namespaceDescriptor = builder.build();
        //同步创建
        admin.createNamespace(namespaceDescriptor);
        System.out.println("同步创建命名空间成功！");
    }

    public void deleteNS(String namespace) throws IOException {
        admin.deleteNamespace(namespace);
        System.out.println("删除命名空间成功！");
    }

    public void createNS_asyn(String namespace) throws Exception {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        //建造者模式
        NamespaceDescriptor descriptor = builder.build();
        //异步创建
        Future<Void> cd = admin.createNamespaceAsync(descriptor);
        Void aVoid = cd.get(10000, TimeUnit.MILLISECONDS);
        System.out.println("异步创建成功！");


    }

    //创建表
    public void createTable(String tableName, String... str) throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String s : str) {
            //添加列族
            descriptor.addFamily(new HColumnDescriptor(s));
        }
        admin.createTable(descriptor);
        System.out.println("表创建成功");
    }

    public void createTable_asyn(String tableName) throws IOException {
        //建造者模式创建   表描述器tabledescriptor
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        //建造者模式创建  列族描述器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

        TableDescriptor tdescriptor = tableDescriptorBuilder.build();
        Future<Void> tableAsync = admin.createTableAsync(tdescriptor);
        System.out.println("异步创建表成功！");

    }

    public void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("删除表成功！");
    }

    //增
    public void insert(String tablename, String rowkey, String colfamily, String quolifier, String value) throws IOException {
//        Put 'staff','1001','info:name','alias'

        Table table = connection.getTable(TableName.valueOf(tablename));
        //获取put对象
        Put put = new Put(rowkey.getBytes());
        put.addColumn(colfamily.getBytes(), quolifier.getBytes(), value.getBytes());
        table.put(put);
        if (flag == true) {
            System.out.println("修改成功！");
            flag = false;
            return;
        } else {
            System.out.println("插入成功！");
        }

    }

    //删
    public void deleteData(String tablename, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Delete delete = new Delete(rowkey.getBytes());
        table.delete(delete);
        System.out.println("删除成功！");

//        table.delete(new Delete());

    }

    //改
    public void alter(String tablename, String rowkey, String colfamily, String quolifier, String value) throws IOException {
        flag = true;
        insert(tablename, rowkey, colfamily, quolifier, value);
        //在没有设置版本时，再次插入及覆盖
    }

    //查
    public void get(String tablename, String rowkey, String colfamily, String quolifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        for (Cell listCell : result.listCells()) {
            System.out.println(Bytes.toString(listCell.getRowArray()));
            System.out.println("\t"+Bytes.toString(listCell.getFamilyArray()));
            System.out.println("\t"+Bytes.toString(listCell.getQualifierArray()));
            System.out.println("\t"+Bytes.toString(listCell.getValueArray()));
        }
/*
        System.out.println("获得" + rowkey + "这行数据：" + "size:" + result.size() + "curent:" + result.current() + "\n"
           + "value:" + Bytes.toString(result.getValue(colfamily.getBytes(), quolifier.getBytes())) + "\n"
        );
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(colfamily.getBytes());
        map.forEach((k, v) -> System.out.println("k:" + Bytes.toString(k) + " " + "v:" + Bytes.toString(v)));

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cqtvMap = result.getMap();
        cqtvMap.forEach((colF, quomap) -> quomap.forEach((quo, tvmap) -> tvmap.forEach((t, v) -> System.out.println("列族:" + Bytes.toString(colF) + "\n" +
           "列：" + Bytes.toString(quo) + "\n" +
           "value:" + Bytes.toString(v)))));*/
    }

    //写过滤器
    public void scan(String tableName, String rowkey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //行键过滤器
        RowFilter rowFilter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator("1000".getBytes()));
        //列族过滤器 familyFilter
        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("s")));
        //列名过滤器   columnPrefixFilter   将列名以"x"开头的通过。
        ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter("x".getBytes());
        //值过滤器
        new ValueFilter(CompareOperator.GREATER,new BinaryComparator(Bytes.toBytes("liu")));
        //随机过滤器
        RandomRowFilter randomRowFilter = new RandomRowFilter(0.5f);
        Scan filter = scan.setFilter(familyFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        //拿到一个cell去遍历
        resultScanner.forEach(i->i.listCells().forEach(cell-> System.out.println("row:"+Bytes.toString(cell.getRowArray()))));
/*        for (Result result : resultScanner) {
            System.out.println(" dao ");
            showbyte(result);
        }*/
    }

    public void showbyte(Result result) {
        Stream<Result> stream = Stream.of(result);
        stream.forEach(value-> System.out.println("rowkey:"+ Bytes.toString(result.getRow())

        ));




    }

    public static void main(String[] args) throws Exception {
        HbaseTest1 test1 = new HbaseTest1();
        test1.getConnection();
//        test1.createNS("java_hbase");
//        test1.deleteNS("java_hbase");
//        test1.createTable("threekingdoms","wei");
//        test1.createTable_asyn("xiyouji");
//        test1.deleteTable("threekingdoms");
//        test1.insert("threekingdoms","1004","wei","花果山","孙悟空");
//        test1.alter("threekingdoms","1001","wei","wu","CHRIS");
//        test1.get("threekingdoms","1001","wei","wu");
        test1.scan("threekingdoms", "1001");
        test1.close();
    }

}
