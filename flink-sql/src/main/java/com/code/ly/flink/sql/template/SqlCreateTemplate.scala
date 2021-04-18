package com.code.ly.flink.sql.template

/**
 * 创建table的 ddl sql  工厂类
 */
object SqlCreateTemplate {
    def  createOrdersFromKafka(topic:String, kafka_server:String):String  = {
        val sql: String =
            s"""
               |create table order (
               |    id string,
               |    item_id,
               |    user_id string,
               |    category string,
               |    order_time timestamp,
               |    WATERMARK FOR order_time AS order_time - INTERVAL '15' SECONDS
               |) with (
               | 'connector' = 'kafka',
               | 'topic' = '${topic}',
               | 'properties.bootstrap.servers' = '${kafka_server}',
               | 'properties.group.id' = '',
               | 'format' = 'json',
               | 'scan.startup.mode' = 'latest-offset'
               |)
               |""".stripMargin
        sql

    }

    /**
     * 用户表 kafka数据源
     * @param topic
     * @param kafka_server
     * @return
     */
    def  createUsersFromKafka(topic:String, kafka_server:String):String  = {
        val sql: String =
            s"""
               |create table users(
               |    id string,
               |    user_name string,
               |    age int
               |) with (
               | 'connector' = 'kafka',
               | 'topic' = '${topic}',
               | 'properties.bootstrap.servers' = '${kafka_server}',
               | 'properties.group.id' = '',
               | 'format' = 'json',
               | 'scan.startup.mode' = 'latest-offset'
               |)
               |""".stripMargin
        sql
    }

    /**
     * 用户表 mysql

     * @return
     */
    def  createUsersFromMysql():String  = {
        val sql: String =
            s"""
               |create table user(
               |    id string,
               |    user_name string,
               |    age int
               |) with (
               |    'connector' = 'jdbc',
               |    'url' = 'jdbc:mysql://localhost:3306/mysql-database'
               |    'table-name' = 'users'
               |    'username' = 'mysql-user'
               |    'password' = 'mysql-password'
               |)
               |""".stripMargin
        sql
    }

    /**
     * 出货表
     * @param topic
     * @param kafka_server
     * @return
     */
    def  createShipmentsFromKafka(topic:String, kafka_server:String):String  = {
        val sql: String =
            s"""
               |create table shipment(
               |id string,
               |shipment_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)) AS INT), CURRENT_TIMESTAMP)
               |process_time as PROCTIME()
               |) with (
               | 'connector' = 'kafka',
               | 'topic' = '${topic}',
               | 'properties.bootstrap.servers' = '${kafka_server}',
               | 'properties.group.id' = '',
               | 'format' = 'json',
               | 'scan.startup.mode' = 'latest-offset'
               |)
               |""".stripMargin
        sql
    }

    /**
     * 用户表 mysql

     * @return
     */
    def  createEmployeesFromMysql():String  = {
        val sql: String =
            s"""
               |create table employees(
               |    id string,
               |    first_name string,
               |    last_name string,
               |    last string,
               |    email string,
               |    salary double,
               |    department_id string
               |) with (
               |    'connector' = 'jdbc',
               |    'url' = 'jdbc:mysql://localhost:3306/mysql-database'
               |    'table-name' = 'employees'
               |    'username' = 'mysql-user'
               |    'password' = 'mysql-password'
               |)
               |""".stripMargin
        sql
    }

    def  createDepartmentsFromMysql():String  = {
        val sql: String =
            s"""
               |create table departments(
               |    id string,
               |    department_name string,
               |    manager_id string
               |) with (
               |    'connector' = 'jdbc',
               |    'url' = 'jdbc:mysql://localhost:3306/mysql-database'
               |    'table-name' = 'departments'
               |    'username' = 'mysql-user'
               |    'password' = 'mysql-password'
               |)
               |""".stripMargin
        sql
    }


}
