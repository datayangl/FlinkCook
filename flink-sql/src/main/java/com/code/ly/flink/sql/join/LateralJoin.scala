package com.code.ly.flink.sql.join

import com.code.ly.flink.sql.template.SqlCreateTemplate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object LateralJoin {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

        tableEnv.execute(SqlCreateTemplate.createOrdersFromKafka("",""))
        tableEnv.execute(SqlCreateTemplate.createEmployeesFromMysql())
        tableEnv.execute(SqlCreateTemplate.createDepartmentsFromMysql())


        // 需求1分析：将表中的data字段分解为2列，其中splitTVF 为UDTF函数
        val sql = "SELECT data, name, age FROM userTab, LATERAL TABLE(splitTVF(data)) AS T(name, age)"

        /**
         * 需求2分析：查找每个部门中薪水最高的 Top 5 和对应的员工
         * 实现思路：可以先按照部门编号对员工信息进行分组，获得每个组内的薪水最高的 5 个员工，但是默认实现回报错，如下：
         * SELECT d.department_name, t.first_name, t.last_name, t.salary
         *  FROM departments d
         *  LEFT JOIN (SELECT e.department_id, e.first_name, e.last_name, e.salary
         *              FROM employees e
         *              WHERE e.department_id = d.department_id
         *              ORDER BY e.salary DESC LIMIT 5) t
         *  ON d.department_id = t.department_id
         * ORDER BY d.department_name, t.salary DESC;
         *
         * ERROR 1054 (42S22): Unknown column 'd.department_id' in 'where clause'
         * 失败的原因在于子查询 t 不能引用外部查询中的 departments 表
         */
        //

        val sql2 =
            """
              | select d.department_name,t.first_name,t.salary
              | from departments d
              | left join LATERAL(
              |     select e.department_id,
              |        e.first_name,
              |        e.last_name,
              |        e.salary
              |      from employees
              |      where e.department_id = d.id
              |      order by e.salary desc limit 5
              |   ) t
              | on d.id = t.department_id
              | order by d.department_name, t.salary DESC;
              |""".stripMargin

    }
}
