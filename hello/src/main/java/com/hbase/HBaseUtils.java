package com.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase工具类
 */
@Slf4j
public class HBaseUtils {
    private static Connection connection;
    
    /**
     * 获取HBase连接
     *
     * @return HBase连接
     */
    public static Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "192.168.100.100");
            try {
                connection = ConnectionFactory.createConnection(conf);
                log.info("创建HBase连接成功！");
                return connection;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("获取HBase连接成功！");
        return connection;
    }
    
    /**
     * 关闭HBase连接
     */
    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                log.info("关闭HBase连接成功！");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    /* ============================================ DDL =========================================== */
    
    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     */
    public static void createNamespace(String namespace) {
        try (Admin admin = connection.getAdmin()) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 判断表格是否存在
     *
     * @param namespace 命名空间
     * @param table     表格
     * @return 表格是否存在
     */
    public static boolean isTableExist(String namespace, String table) {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 创建表格
     *
     * @param namespace      命名空间
     * @param table          表格
     * @param columnFamilies 列族
     */
    public static void createTable(String namespace, String table, String... columnFamilies) {
        if (columnFamilies.length == 0) {
            throw new RuntimeException("表格至少需要一个列族");
        }
        try (Admin admin = connection.getAdmin()) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                        ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes(StandardCharsets.UTF_8));
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 修改表格
     *
     * @param namespace    命名空间
     * @param table        表格
     * @param columnFamily 列族
     * @param maxVersion   最大版本
     */
    public static void modifyTable(String namespace, String table, String columnFamily, int maxVersion) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(namespace, table));
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableDescriptor);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(tableDescriptor.getColumnFamily(columnFamily.getBytes(StandardCharsets.UTF_8)))
                                                 .setMaxVersions(maxVersion);
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 删除表格
     *
     * @param namespace 命令空间
     * @param table     表格
     * @return 是否删除成功
     */
    public static boolean deleteTable(String namespace, String table) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Admin admin = connection.getAdmin()) {
            try {
                // 先禁用
                admin.disableTable(TableName.valueOf(namespace, table));
                // 再删除
                admin.deleteTable(TableName.valueOf(namespace, table));
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /* ============================================ DML =========================================== */
    
    /**
     * 写入单元格
     *
     * @param namespace       命名空间
     * @param table           表格
     * @param rowKey          RowKey
     * @param columnFamily    列族
     * @param columnQualifier 列限定符
     * @param value           数据
     */
    public static void put(String namespace, String table, String rowKey, String columnFamily, String columnQualifier, String value) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Table t = connection.getTable(TableName.valueOf(namespace, table))) {
            Put put = new Put(rowKey.getBytes(StandardCharsets.UTF_8));
            put.addColumn(columnFamily.getBytes(StandardCharsets.UTF_8), columnQualifier.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            t.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 读取单元格
     *
     * @param namespace       命名空间
     * @param table           表格
     * @param rowKey          RowKey
     * @param columnFamily    列族
     * @param columnQualifier 列限定符
     * @return 数据列表
     */
    public static List<String> get(String namespace, String table, String rowKey, String columnFamily, String columnQualifier) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Table t = connection.getTable(TableName.valueOf(namespace, table))) {
            Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
            get.addColumn(columnFamily.getBytes(StandardCharsets.UTF_8), columnQualifier.getBytes(StandardCharsets.UTF_8))
               .readAllVersions();
            Result result = t.get(get);
            List<Cell> cellList = result.listCells();
            ArrayList<String> list = new ArrayList<>();
            for (Cell cell : cellList) {
                list.add(new String(CellUtil.cloneValue(cell)));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 删除单元格
     *
     * @param namespace       命名空间
     * @param table           表格
     * @param rowKey          RowKey
     * @param columnFamily    列族
     * @param columnQualifier 列限定符
     */
    public static void delete(String namespace, String table, String rowKey, String columnFamily, String columnQualifier) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Table t = connection.getTable(TableName.valueOf(namespace, table))) {
            Delete delete = new Delete(rowKey.getBytes(StandardCharsets.UTF_8));
            delete.addColumn(columnFamily.getBytes(StandardCharsets.UTF_8), columnQualifier.getBytes(StandardCharsets.UTF_8));
            t.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    /**
     * 扫描表格
     *
     * @param namespace 命名空间
     * @param table     表格
     * @param startRow  startRow
     * @param stopRow   stopRow
     * @return 数据列表
     */
    public static List<String> scan(String namespace, String table, String startRow, String stopRow) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Table t = connection.getTable(TableName.valueOf(namespace, table))) {
            Scan scan = new Scan();
            scan.withStartRow(startRow.getBytes(StandardCharsets.UTF_8))
                .withStopRow(stopRow.getBytes(StandardCharsets.UTF_8));
            ResultScanner scanner = t.getScanner(scan);
            ArrayList<String> list = new ArrayList<>();
            for (Result result : scanner) {
                list.add(new String(result.value()));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 扫描表格
     *
     * @param namespace       命名空间
     * @param table           表格
     * @param columnFamily    列族
     * @param columnQualifier 列限定名
     * @param value           数据
     * @param startRow        startRow
     * @param stopRow         stopRow
     * @return 数据列表
     */
    public static List<String> filterScan(String namespace, String table, String columnFamily, String columnQualifier, String value, String startRow, String stopRow) {
        if (!isTableExist(namespace, table)) {
            throw new RuntimeException("表格不存在");
        }
        try (Table t = connection.getTable(TableName.valueOf(namespace, table))) {
            Scan scan = new Scan();
            scan.withStartRow(startRow.getBytes(StandardCharsets.UTF_8))
                .withStopRow(stopRow.getBytes(StandardCharsets.UTF_8))
                .setFilter(new ColumnValueFilter(
                        columnFamily.getBytes(StandardCharsets.UTF_8),
                        columnQualifier.getBytes(StandardCharsets.UTF_8),
                        CompareOperator.EQUAL,
                        value.getBytes(StandardCharsets.UTF_8)
                ));
            ResultScanner scanner = t.getScanner(scan);
            ArrayList<String> list = new ArrayList<>();
            for (Result result : scanner) {
                list.add(new String(result.value()));
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
