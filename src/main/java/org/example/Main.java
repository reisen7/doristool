package org.example;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.SqlParserExample.getTableName;

public class Main {

    public static void main(String[] args) throws Exception {
        initLogRecord.initLog();
        System.out.println("================================================================");
        Connection conn = HikariConfigs.HikariConn();
        // Statement stmt = conn.createStatement();
        Scanner scanner = new Scanner(System.in);
        System.out.println("===========================请输入删除的语句=========================");
        String sql = scanner.nextLine();
        String where = "";
        if (sql.indexOf("where") > 0) {
            where = sql.substring(sql.indexOf("where"));
        } else if (sql.indexOf("WHERE") > 0) {
            where = sql.substring(sql.indexOf("WHERE"));
        }
        System.out.println("=======================获取WHERE条件=============================");
        System.out.println(where);
        String table = getTableName(sql);
        String getKey = "select * from information_schema.columns where TABLE_NAME = ? ";
        PreparedStatement ptmt_getKey = conn.prepareStatement(getKey);
        ptmt_getKey.setString(1, table);
        ResultSet rs = ptmt_getKey.executeQuery();
        List<String> key = new ArrayList<>();
        Map<String, String> key_type = new HashMap<>();
        System.out.println("=======================获取KEY==================================");
        while (rs.next()) {
            if (rs.getString("column_key").equals("UNI")) {
                key.add(rs.getString("column_name"));
                key_type.put(rs.getString("column_name"), rs.getString("DATA_TYPE"));
                System.out.println(rs.getString("column_name") + "  " + rs.getString("column_key"));
            }
        }

        System.out.println("=======================拼接key==================================");
        String column = "";
        for (int i = 0; i < key.size(); i++) {
            if (column != "") {
                column = column + "," + key.get(i);
            } else {
                column = key.get(i);
            }
        }
        System.out.println(column);

        // 拼接SELECT 语句
        String newSQL = "SELECT " + column + " FROM " + table + " " + where;
        PreparedStatement pstm_getkeyvalue = conn.prepareStatement(newSQL);
        rs = pstm_getkeyvalue.executeQuery();
        List<Map<String, String>> values = new ArrayList<>();
        while (rs.next()) {
            Map<String, String> map = new HashMap<>();
            for (String x : key) {
                map.put(x, rs.getString(x));
            }
            values.add(map);
        }
        // for (int i =0 ;i< values.size();i++){
        // System.out.println(values.get(i));;
        // }

        // 循环拼接删除的where 条件
        StringBuilder delete_where = new StringBuilder();
        String delete_sql = "";
        List<String> delete_sqls = new ArrayList<>();
        for (Map<String, String> x : values) {
            delete_where = new StringBuilder();
            for (String y : x.keySet()) {
                if (delete_where.length() > 0) {
                    // 时间格式的拼接
                    if (key_type.get(y).equals("datetime")) {
                        LocalDateTime date = LocalDateUtils.parseLocalDateTime(x.get(y),
                                LocalDateUtils.DATETIME_PATTERN);
                        String str = LocalDateUtils.format(date, LocalDateUtils.UNSIGNED_DATETIME_PATTERN);
                        delete_where.append(" AND ").append(y).append("=").append("'").append(str).append("'");
                    } else {
                        delete_where.append(" AND ").append(y).append("=").append("'").append(x.get(y)).append("'");
                    }
                } else {
                    if (key_type.get(y).equals("datetime")) {
                        LocalDate date = LocalDateUtils.parseLocalDate(x.get(y), LocalDateUtils.DATETIME_PATTERN);
                        String str = LocalDateUtils.format(date, LocalDateUtils.UNSIGNED_DATETIME_PATTERN);
                        delete_where = new StringBuilder(y + "=" + "'" + str + "'");
                    } else {
                        delete_where = new StringBuilder(y + "=" + "'" + x.get(y) + "'");
                    }
                }
            }
            delete_sql = "";
            delete_sql = "DELETE FROM " + table + " WHERE " + delete_where;
            delete_sqls.add(delete_sql);
            // System.out.println("删除语句："+ delete_sql);
            // if (dels.executeUpdate(delete_sql)>0){
            // i++;
            // System.out.println("=======================删除成功=======================");
            // }else{
            // System.out.println("!!!!!!!!!!!!!!!!!删除失败!!!!!!!!!!!!!!!!!");
            // j++;
            // }
        }
        conn.close();

        // 使用线程执行sql
        System.out.println("=====================  需要删除数据的总数为: " + delete_sqls.size() + "  =================");
        System.out.println("======================  请确认是否删除？ yes or no=====================");
        String is_delete = scanner.nextLine();
        if (!is_delete.equals("yes")) {
            return;
        }

        // ExecutorService executor = Executors.newFixedThreadPool(10); // 创建一个固定大小的线程池
        // AtomicInteger successCount = new AtomicInteger(0); // 原子计数器，用于记录成功执行的SQL数量
        // AtomicInteger failureCount = new AtomicInteger(0); // 统计失败次数
        // long start = System.currentTimeMillis();
        // executor.submit(() -> {
        //     try (Connection connection = HikariConfigs.HikariConn();) {
        //         for (int i = 0; i < delete_sqls.size(); i++) {
        //             Statement statement = connection.createStatement();
        //             String deletesql = delete_sqls.get(i);
        //             System.out.println("!!!正在删除数据：" + deletesql);
        //             // int updateCount = statement.executeUpdate(deletesql);
        //             // if (updateCount > -1) {
        //             //     successCount.incrementAndGet(); // 如果删除成功，增加本地计数器

        //             // } else {
        //             //     failureCount.incrementAndGet();// 如果删除失败
        //             // }

        //             statement.addBatch(deletesql);
        //             successCount.incrementAndGet(); // 如果删除成功，增加本地计数器
        //             if (i % 500 == 0) {
        //                 statement.executeBatch();
        //             }
        //         }
        //     } catch (SQLException e) {
        //         e.printStackTrace();
        //     } catch (InterruptedException ex) {
        //         ex.printStackTrace();
        //     }

        // });

        // executor.shutdown(); // 关闭线程池
        // executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // 等待所有任务完成
        // long end = System.currentTimeMillis();

        
        AtomicInteger successCount = new AtomicInteger(0); // 原子计数器，用于记录成功执行的SQL数量
        AtomicInteger failureCount = new AtomicInteger(0); // 统计失败次数

        int size = delete_sqls.size();
        int theadCount = 10;
        int splitCount = size / theadCount + (size % theadCount != 0 ? 1 : 0); //计算分拆数量，向上取整
        final CountDownLatch cdl = new CountDownLatch(theadCount);// 定义线程数量
        long starttime = System.currentTimeMillis();
        for (int k = 1; k <= theadCount; k++) {
            final int beign = (k - 1)  * splitCount;
            final int end = (k * splitCount) > size ? size : k * splitCount;
            if(beign >= end) break;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection con = JDBCTools.getConnection();
                    try {
                        Statement st = con.createStatement();
                        for (int i = 0; i < delete_sqls.size(); i++) {
                            String deletesql = delete_sqls.get(i);
                            System.out.println("!!!正在删除数据：" + deletesql);
                            int count = st.executeUpdate(deletesql);
                            if (count > -1) {
                                successCount.incrementAndGet();
                            }else{
                                failureCount.incrementAndGet();
                            }
                        }
                        cdl.countDown(); // 执行完一个线程，递减1
                    } catch (Exception e) {
                    } finally {
                        try {
                            con.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
        try {
            cdl.await();    //前面线程没执行完，其他线程等待，不往下执行
            long spendtime=System.currentTimeMillis()-starttime;
            System.out.println( theadCount+"个线程花费时间:"+spendtime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println("已完成删除");
        System.out.println("成功删除数据 " + successCount + " 条");
        System.out.println("删除失败 " + failureCount + " 条");
        System.out.println("消耗的时间为（毫秒）：" + (endtime - starttime));
        System.out.println("消耗的时间为（秒）：" + TimeUnit.MILLISECONDS.toSeconds(endtime - starttime));

    }
}