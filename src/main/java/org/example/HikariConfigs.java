package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HikariConfigs {

    public static String URL = "jdbc:mysql://localhost:3306/imooc";
    public static String USER = "sigtuna";
    public static String PASSWORD = "123456";
    public static String CLASSNAME = "com.mysql.jdbc.Driver";
    private static HikariDataSource hikariDataSource;
    public static HikariConfig config = new HikariConfig();

    // 单例模式创建hikari
    private HikariConfigs() {

    }

    static {
        // 配置数据库连接信息
        InputStream inputStream;
        inputStream = Main.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        URL = properties.getProperty("URL");
        USER = properties.getProperty("USER");
        PASSWORD = properties.getProperty("PASSWORD");
        CLASSNAME = properties.getProperty("DRIVER");

    }

    // private static class HikariLoader {
    // // 定义静态常量
    // private static final HikariConfigs INSTANCE = new HikariConfigs();
    //
    // }
    //
    // public static HikariConfigs getInstance() {
    // // 返回单例对象
    // return HikariLoader.INSTANCE;
    // }

    // 线程安全构造单例模式
    public static synchronized HikariDataSource getInstance() {

        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setDriverClassName(CLASSNAME);
        config.setAutoCommit(true);
        config.setPoolName("laker_poolName");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(3);

        hikariDataSource = new HikariDataSource(config);

        return hikariDataSource;
    }

    @Deprecated
    public static HikariDataSource HDataSource() {

        InputStream inputStream;
        inputStream = Main.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String apUrl = properties.getProperty("URL");
        String apUser = properties.getProperty("USER");
        String apPwd = properties.getProperty("PASSWORD");
        String apDriver = properties.getProperty("DRIVER");

        config.setJdbcUrl(apUrl);
        config.setUsername(apUser);
        config.setPassword(apPwd);
        config.setDriverClassName(apDriver);
        config.setAutoCommit(true);
        config.setPoolName("laker_poolName");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(3);
        HikariDataSource dataSource;
        if (hikariDataSource == null) {
            dataSource = new HikariDataSource(config);
        } else {
            dataSource = hikariDataSource;
        }
        return dataSource;
    }

    public static Connection HikariConn() {

        HikariDataSource dataSource = HikariConfigs.getInstance();
        // poolState();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }

    // hikari 监控
    public static void poolState() throws InterruptedException, SQLException {
        HikariDataSource dataSource = HikariConfigs.HDataSource();
        LoggingMeterRegistry loggingMeterRegistry = new LoggingMeterRegistry(new LoggingRegistryConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(10);
            }
        }, Clock.SYSTEM);
        dataSource.setMetricRegistry(loggingMeterRegistry);
        Connection connection = dataSource.getConnection();
        TimeUnit.SECONDS.sleep(3);
        connection.close();
    }

    public static String getURL() {
        return URL;
    }

    public static void setURL(String URL) {
        HikariConfigs.URL = URL;
    }

    public static String getUSER() {
        return USER;
    }

    public static void setUSER(String USER) {
        HikariConfigs.USER = USER;
    }

    public static String getPASSWORD() {
        return PASSWORD;
    }

    public static void setPASSWORD(String PASSWORD) {
        HikariConfigs.PASSWORD = PASSWORD;
    }

    public static String getCLASSNAME() {
        return CLASSNAME;
    }

    public static void setCLASSNAME(String CLASSNAME) {
        HikariConfigs.CLASSNAME = CLASSNAME;
    }
}
