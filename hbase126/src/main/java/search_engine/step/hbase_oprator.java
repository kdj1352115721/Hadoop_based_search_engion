package search_engine.step;
// hbase 172.16.0.4:16010   webpage_aliyun
//zookeeper
//  172.16.0.4:2181  172.16.0.5:2181  172.16.0.6:2181  172.16.0.7:2181
//数据清洗
// 1.从抓取过的页面中（f:st=2）   选择 列族：列

//2.提取有效字段（url,title,il , ol）  字段
// il 入链接的内容
// ol 出链接的对象，个数

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;

//out    kangdaojian_Step1_dataClean
// hbase ---->  clean  ----> hbase（建表）
public class hbase_oprator {
    private static Configuration conf;
    private static Connection conn;
    private static Admin admin;
    private static Table table;
    private static Scan scan;
    private static String tablename = "aliyun_webpage";
    private static String url;
    private static String title;
    private static String il;
    private static String ol;

    public void getConnect() throws Exception {
        //拿到配置对象
        conf = HBaseConfiguration.create();
//        172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181
        conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //获取连接
        conn = ConnectionFactory.createConnection(conf);
        System.out.println("已连接" + conn);
        //拿到对象
        admin = conn.getAdmin();
        System.out.println("已拿到对象！" + admin);

    }

    public void createNS() throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("kdj_search");
        NamespaceDescriptor namespaceDescriptor = builder.build();

        admin.createNamespace(namespaceDescriptor);
        System.out.println("创建命名空间成功！");
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
    public void showtable(String tablename) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));
        System.out.println(table.getClass());
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
        System.out.println(families.size());
    }
/*
    public void getfilter() throws IOException {
        StringBuffer buffer = new StringBuffer();
        table = conn.getTable(TableName.valueOf(tablename));
        scan = new Scan();
        //列族过滤器，得到指定列族"f"
        Filter family_f_Filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("f")));
        if (family_f_Filter != null) {
            System.out.println("已经获取到了列族f");
            foreach(setfilter(family_f_Filter));
            //列过滤器，得到指定列“url”  “f”->"bas"
            System.out.println("即将获取列bas");
//            ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("bas"));
            QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("bas")));
            System.out.println("已经获取到了列bas");
            ResultScanner results = setfilter(qualifierFilter);
            foreach(results);
            //等待插入hbase,先保存在buffer中
            results.forEach(result -> result.listCells().forEach(cell -> buffer.append(cell.getQualifierArray() + ",")));
            //列过滤器, 得到指定列“st”,成功抓取的页面
        }
        //列过滤器，得到指定列“title”  "p" -> "t"
        FamilyFilter family_p_Filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("p")));
        if (family_p_Filter != null) {
            System.out.println("已经获取到了列族p");
            foreach(setfilter(family_p_Filter));
            System.out.println("即将获取列title");
//            ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("t"));
            QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("t")));
            ResultScanner results = setfilter(qualifierFilter);
            foreach(results);
            //等待插入hbased
            results.forEach(result -> result.listCells().forEach(cell -> buffer.append(cell.getQualifierArray())));
            insert(buffer);  //插入作为行键
        }
        //列过滤器，得到指定列"il"      "il"
        FamilyFilter family_il_Filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("il")));
        if (family_il_Filter != null) {
            System.out.println("已经获取到了列族il");
            foreach(setfilter(family_il_Filter));
            //等待插入hbase
        }
        //列过滤器，得到指定列“ol”，   "ol"      个数、对象
        FamilyFilter family_ol_Filter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("ol")));
        System.out.println("已经获取到了列族ol");
        foreach(setfilter(family_ol_Filter));
        //等待插入hbase

    }*/

    public ResultScanner setfilter(Filter filter) throws IOException {
        //将过滤器设置进去，并返回遍历需要的resultscanner
        Scan scan = hbase_oprator.scan.setFilter(filter);
        return table.getScanner(scan);
    }

    public void foreach(ResultScanner results) {
        //打印出行键
        results.forEach(result -> result.listCells()
           .forEach(cell -> System.out.println("row:" + Bytes.toString(cell.getRowArray()) + "\n" +
              "family:" + Bytes.toString(cell.getFamilyArray()) + "\n" +
              "qualify:" + Bytes.toString(cell.getQualifierArray()) + "\n" +
              "value:" + Bytes.toString(cell.getValueArray())
           )));
        System.out.println("==========================================");
    }

    public void insert(StringBuffer sb) throws IOException {
        Put put = new Put(Bytes.toBytes(sb.toString()));
        table.put(put);
        System.out.println("已经插入行键");
    }
    public void get(String tablename, String rowkey, String family, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tablename));
/*
        for (Cell cell : result.listCells()) {
            byte[] rowArray = cell.getRowArray();
            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] value = cell.getValue();

        }
*/

    }
    public static void main(String[] args) throws Exception {
        hbase_oprator dataClean = new hbase_oprator();
        dataClean.getConnect();
//        dataClean.createNS();
        dataClean.createTable("table_afterrank", "info");
//        dataClean.showtable("table_afterclean");
//        dataClean.getfilter();

    }
}
