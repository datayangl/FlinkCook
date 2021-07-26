package com.code.ly.flink.dev.sql;

import com.code.ly.flink.dev.config.ConfigManager;
import com.code.ly.flink.dev.env.FlinkBatchEnv;
import com.code.ly.flink.dev.util.StringUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;

/**
 * sql执行的公共jar
 * @author luoy
 */
public class SqlExecApp {
    private static final String DEFAULT_HIVE_CATALOG_NAME = "myHive";
    private static final String DEFAULT_HIVE_DATABASE = "default";
    private static final String DEFAULT_HIVE_CONF_DIR = "/etc/hive/conf";

    private static final String FLINK_SQL_EXEC_MODE_KEY = "flink.sql.exec.mode";
    private static final String FLINK_SQL_EXEC_SQL_KEY = "flink.sql.exec.sql";
    public static void main(String[] args) {
        String str = "insert into  luoy.student_info select student.id,student.info from luoy2.student left join luoy.student_loc on student.id = student_loc.id";

        String union_sql = "insert into luoy.student_union select row_number over (partition by )student.id,student.info from luoy.student union select student2.id,student2.info from luoy.student2";

        String alter_Sql = "alter table luoy.test rename to luoy.test2";
        String sort_sql = "insert into luoy.student_sort_result select tmp.id,tmp.info,tmp.age from (select *,row_number() over (partition by age order by id) num from luoy.student_sort) tmp where tmp.num=1 ";

        String[] a = new String[]{"--flink.sql.exec.mode","batch","--flink.sql.exec.sql", alter_Sql};
        /** 1.获取运行配置 **/
        Properties props = ConfigManager.loadFromArgs(a);

        /** 2.配置检查 **/
        preCheck(props);

        /** 3.执行 **/
        String mode = props.getProperty(FLINK_SQL_EXEC_MODE_KEY);

        if ("stream".equals(mode)) {
            runStreamSql(props);
        }

        if ("batch".equals(mode)) {
            runBatchSql(props);
        }

        String sql = props.getProperty(FLINK_SQL_EXEC_SQL_KEY, "");
        if (sql == null || "".equals(sql)) {
            throw new IllegalArgumentException("待执行sql为空");
        }

    }

    public static void runStreamSql(Properties props) {

    }

    public static void runBatchSql(Properties props) {
        FlinkBatchEnv flinkBatchEnv = new FlinkBatchEnv(FlinkBatchEnv.BatchMode.BATCH_TABLE);
        TableEnvironment tableEnv = flinkBatchEnv.getBatchTableEnv();

        HiveCatalog hiveCatalog = new HiveCatalog(DEFAULT_HIVE_CATALOG_NAME, DEFAULT_HIVE_DATABASE, "/Users/luoy/Desktop/doc/script");
        tableEnv.registerCatalog(DEFAULT_HIVE_CATALOG_NAME, hiveCatalog);
        tableEnv.useCatalog(DEFAULT_HIVE_CATALOG_NAME);
        //tableEnv.useDatabase("luoy");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String sql = props.getProperty(FLINK_SQL_EXEC_SQL_KEY);
        TableResult result = tableEnv.executeSql(sql);
        System.out.println(result.getJobClient().get().getJobStatus());
    }

    /**
     * 前置检查
     * @param props
     */
    public static void preCheck(Properties props) {
        if (!props.containsKey(FLINK_SQL_EXEC_MODE_KEY)) {
            throw new IllegalArgumentException("[error]:flink sql exec mode not set");
        }

        if (!props.containsKey(FLINK_SQL_EXEC_SQL_KEY) || StringUtil.isBlank(props.getProperty(FLINK_SQL_EXEC_SQL_KEY))) {
            throw new IllegalArgumentException("[error]:flink sql is empty");
        }
    }
}
