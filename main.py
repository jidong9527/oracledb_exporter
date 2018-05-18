# -*- coding:utf-8 -*-
# (c) 2018, Ji Dongdong <jidongdong@cnnic.cn>
#  
# 模块用于收集oracledb相关信息,导出给prometheus

import time
import logging
import cx_Oracle
import os
import threading
from multiprocessing import Pool
from dotenv import load_dotenv
from prometheus_client import start_http_server, Gauge
from prometheus_client.core import Summary

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)
# 设置监听的web服务器端口
HTTP_PORT = int(os.environ.get("HTTP_PORT"))
# 设置oracle连接用户名密码
DATA_SOURCE_NAME = os.environ.get("DATA_SOURCE_NAME")
# 设置日志格式
LOG_LEVEL = os.environ.get("LOG_LEVEL")
FORMAT = '%(asctime)-15s %(thread)-5d:%(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('oracledb_exporter')
logger.setLevel(LOG_LEVEL)
# 收集数据所用时长
COLLECTION_TIME = Summary(
    'oracledb_collector_collect_seconds',
    'Time spent to collect metrics from Oracle'
)
SCRAPE_INTERVAL = float(os.environ.get("SCRAPE_INTERVAL"))


class OracleCollector(object):

    def __init__(self):
        self.db_connect, self.db_cursor = '', ''
        self.database_version_gauge = Gauge(
            "oracledb_version_info",
            ""
            "TYPE gauge.",
            ["version"]
        )
        # ["oracle", "plsql", "core", "tns", "nlsrtl"],
        self.database_registry_gauge = Gauge(
            "oracledb_database_registry_info",
            ""
            "TYPE gauge.",
            ["comp_id", "comp_name", "version", "status", "modified",
             "control", "schema", "procedure"]
        )
        self.high_water_mark_statistics_gauge = Gauge(
            "oracledb_dba_high_water_mark_statistics_info",
            "",
            ["statistic_name", "highwater", "last_value", "description"]
        )
        self.instance_overview_gauge = Gauge(
            "oracledb_instance_overview_info",
            "",
            ["name", "db_unique_name", "dbid", "open_mode", "created",
             "platform_name", "database_role", "controlfile_type",
             "current_scn", "log_mode", "FORCE_LOGGING", "flashback_on"]
        )
        self.database_overview_gauge = Gauge(
            "oracledb_database_overview_info",
            "",
            [
                "name", "db_unique_name", "dbid", "open_mode", "created",
                "platform_name", "database_role", "controlfile_type",
                "current_scn", "log_mode", "FORCE_LOGGING", "flashback_on"
            ]
        )
        self.initialization_parameters_gauge = Gauge(
            "oracledb_initialization_parameters_info",
            "",
            ["spfile"]
        )
        self.control_files_gauge = Gauge(
            "oracledb_control_files_info",
            "",
            ["name", "status", "file_size"]
        )
        self.online_redo_logs_gauge = Gauge(
            "oracledb_online_redo_logs_info",
            "",
            [
                "instance_name_print", "thread_number_print", "groupno",
                "member", "redo_file_type", "log_status", "archived", "bytes"
            ]
        )
        self.redo_log_switches_gauge = Gauge(
            "oracledb_redo_log_switches_info",
            "",
            ["date", "total"]
        )
        self.tablespace_status_gauge = Gauge(
            "oracledb_tablespace_status_info",
            "",
            [
                "tablespace_name", "tablespace_size", "tablespace_used",
                "tablespace_free", "tablespace_used_ratio"
            ]
        )
        self.invalid_objects_gauge = Gauge(
            "oracledb_invalid_objects_info",
            "",
            [
                "owner", "object_name", "object_type"
            ]
        )
        self.invalid_indexes_gauge = Gauge(
            "oracledb_invalid_indexes_info",
            "",
            [
                "owner", "index_name", "table_name"
            ]
        )
        self.active_sql_gauge = Gauge(
            "oracledb_active_sql_info",
            "",
            [
                "sid", "username", "osuser", "machine", "module", "status",
                "optimizer_mode", "sql_text"
            ]
        )
        self.dataguard_master_status_gauge = Gauge(
            "oracledb_dataguard_master_status_info",
            "",
            [
                "group", "thread", "sequence", "size_mb", "status"
            ]
        )
        self.dataguard_slave_status_gauge = Gauge(
            "oracledb_dataguard_slave_status_info",
            "",
            [
                "thread", "sequence"
            ]
        )

    def collect(self):
        start = time.time()
        try:
            self.db_connect, self.db_cursor = self.get_conn_and_cursor()
            self.database_version()
            self.database_registry()
            self.high_water_mark_statistics()
            self.instance_overview()
            self.database_overview()
            self.initialization_parameters()
            self.control_files()
            self.online_redo_logs()
            self.redo_log_switches()
            self.tablespace_status()
            self.invalid_objects()
            self.invalid_indexes()
            self.active_sql()
            self.dataguard_master_status()
            self.dataguard_slave_status()
        except Exception as e:
            print str(e)
        self.close_db()

        duration = time.time() - start
        print "Total time: %.3f s" % duration
        COLLECTION_TIME.observe(duration)

    def get_conn_and_cursor(self):
        conn = cx_Oracle.connect(DATA_SOURCE_NAME)
        curs = conn.cursor()
        return conn, curs

    def close_db(self):
        # 关闭游标
        self.db_cursor.close()
        # 关闭DB连接
        self.db_connect.close()

    def database_version(self):
        logger.info("collect oracledb_version_info")
        try:
            sql = "SELECT * FROM v$version"
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for i in range(len(rows)):
                self.database_version_gauge.labels(rows[i][0]).set(1)
        except Exception as e:
            logger.error(str(e))

    def database_registry(self):
        logger.info("collect oracledb_database_registry_info")
        try:
            sql = """SELECT comp_id,comp_name,version,status,modified,control,
                     schema,procedure 
                     FROM dba_registry ORDER BY comp_name"""

            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.database_registry_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                    row[7]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def high_water_mark_statistics(self):
        logger.info("collect oracledb_dba_high_water_mark_statistics_info")
        try:
            sql = """SELECT name statistic_name, highwater, last_value, 
                     description 
                     FROM dba_high_water_mark_statistics ORDER BY name"""
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.high_water_mark_statistics_gauge.labels(
                    row[0], row[1], row[2], row[3]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def instance_overview(self):
        logger.info("collect oracledb_instance_overview_info")
        try:
            sql = """SELECT name,db_unique_name, dbid, open_mode, created,
                     platform_name, database_role, controlfile_type, current_scn,
                     log_mode, FORCE_LOGGING, flashback_on 
                     FROM v$database"""
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.instance_overview_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4], row[5],
                    row[6], row[7], row[8], row[9], row[10], row[11]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def database_overview(self):
        logger.info("collect oracledb_database_overview_info")
        try:
            sql = """
                SELECT name,db_unique_name,dbid,open_mode,created,
                platform_name,database_role,controlfile_type,
                current_scn,log_mode,FORCE_LOGGING,flashback_on 
                FROM v$database
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.database_overview_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4], row[5],
                    row[6], row[7], row[8], row[9], row[10], row[11]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def initialization_parameters(self):
        logger.info("collect oracledb_initialization_parameters_info")
        try:
            sql = """
                SELECT 'This database '|| DECODE( (1-SIGN(1-SIGN(count(*) - 0))),1 , 'IS' , 'IS NOT') || 
                ' using an SPFILE.'spfile  
                FROM v$spparameter 
                WHERE value IS NOT NULL
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.initialization_parameters_gauge.labels(row[0]).set(1)
        except Exception as e:
            logger.error(str(e))

    def control_files(self):
        logger.info("collect oracledb_control_files_info")
        try:
            sql = "SELECT name,DECODE( c.status,NULL,'VALID','' || c.status || '') status,''|| " \
                  "TO_CHAR(block_size * file_size_blks, '999,999,999,999') || '' file_size  " \
                  "FROM v$controlfile c ORDER BY c.name"
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.control_files_gauge.labels(
                    row[0], row[1], row[2]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def online_redo_logs(self):
        logger.info("collect oracledb_online_redo_logs_info")
        try:
            sql = """
                SELECT '' || i.instance_name || '' instance_name_print, '' || i.thread# || '' thread_number_print,
                f.group# groupno,'' || f.member || '' member,f.type redo_file_type,
                DECODE( l.status,'CURRENT','' || l.status || '','' || l.status || '') log_status,l.bytes bytes,
                '' || l.archived || '' archived FROM gv$logfile f ,gv$log l, gv$instance i WHERE
                f.group# = l.group# AND l.thread# = i.thread# AND i.inst_id = f.inst_id AND f.inst_id = l.inst_id
                ORDER BY i.instance_name , f.group# , f.member
            """

            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.online_redo_logs_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def redo_log_switches(self):
        logger.info("collect oracledb_redo_log_switches_info")
        try:
            sql = """
                SELECT ''|| SUBSTR(TO_CHAR(first_time, 'yyyy-MM-DD HH:MI:SS'),1,7) ||'' "DATE",count(*) TOTAL  
                FROM v$log_history 
                GROUP BY SUBSTR(TO_CHAR(first_time, 'yyyy-MM-DD HH:MI:SS'),1,7) 
                ORDER BY SUBSTR(TO_CHAR(first_time, 'yyyy-MM-DD HH:MI:SS'),1,7)
            """

            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.redo_log_switches_gauge.labels(row[0], row[1]).set(1)
        except Exception as e:
            logger.error(str(e))

    def tablespace_status(self):
        logger.info("collect oracledb_tablespace_status_info")
        try:
            sql = """
                SELECT UPPER(F.TABLESPACE_NAME) "tablespace_name",
                        D.TOT_GROOTTE_MB "tablespace_size(M)",
                        D.TOT_GROOTTE_MB-F.TOTAL_BYTES "tablespace_used(M)",
                        F.TOTAL_BYTES "tablespace_free(M)",
                        TO_CHAR(ROUND((D.TOT_GROOTTE_MB-F.TOTAL_BYTES)/D.TOT_GROOTTE_MB*100,2),'990.99') "tablespace_used%"
                FROM (SELECT TABLESPACE_NAME,
                          ROUND(SUM(BYTES)/1024/1024) TOTAL_BYTES,
                          ROUND(MAX(BYTES)/(1024*1024),2) MAX_BYTES
                       FROM SYS.DBA_FREE_SPACE
                       GROUP BY TABLESPACE_NAME) F,
                     (SELECT DD.TABLESPACE_NAME,
                          ROUND(SUM(BYTES)/1024/1024) TOT_GROOTTE_MB
                       FROM SYS.DBA_DATA_FILES DD
                       GROUP BY DD.TABLESPACE_NAME) D
                WHERE D.TABLESPACE_NAME=F.TABLESPACE_NAME
                ORDER BY 2 DESC
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.tablespace_status_gauge.labels(row[0], row[1], row[2], row[3], row[4]).set(1)
        except Exception as e:
            logger.error(str(e))

    def invalid_objects(self):
        logger.info("collect oracledb_invalid_objects_info")
        try:
            sql = "SELECT owner,object_name,object_type FROM dba_objects WHERE status = 'INVALID'"
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.invalid_objects_gauge.labels(row[0], row[1], row[2]).set(1)
        except Exception as e:
            logger.error(str(e))

    def invalid_indexes(self):
        logger.info("collect oracledb_invalid_indexes_info")
        try:
            sql = """SELECT OWNER,INDEX_NAME,TABLE_NAME 
                     FROM dba_indexes 
                     WHERE status <> upper('VALID') AND OWNER NOT LIKE 'SYS%'
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.invalid_indexes_gauge.labels(row[0], row[1], row[2]).set(1)
        except Exception as e:
            logger.error(str(e))

    def active_sql(self):
        logger.info("collect oracledb_active_sql_info")
        try:
            sql = """
                SELECT sesion.sid, username, osuser, machine, sesion.module, 
                       status, optimizer_mode, sql_text
                FROM v$sqlarea sqlarea, v$session sesion 
                WHERE sesion.sql_hash_value = sqlarea.hash_value(+) 
                      AND sesion.sql_address = sqlarea.address(+) 
                      AND sesion.username IS NOT NULL 
                      AND status = 'ACTIVE'
                ORDER BY username, sql_text
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.active_sql_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def dataguard_master_status(self):
        logger.info("collect oracledb_dataguard_master_status_info")
        try:
            sql = """
                SELECT a.GROUP#,THREAD#,SEQUENCE#,BYTES/1024/1024 "bytes(M)",a.status 
                FROM v$log a,v$logfile b 
                WHERE a.GROUP#=b.GROUP# AND a.status='CURRENT' ORDER BY 1,2
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.dataguard_master_status_gauge.labels(
                    row[0], row[1], row[2], row[3], row[4]
                ).set(1)
        except Exception as e:
            logger.error(str(e))

    def dataguard_slave_status(self):
        logger.info("collect oracledb_dataguard_slave_status_info")
        try:
            sql = """
                SELECT THREAD#, MAX(SEQUENCE#) "SEQUENCE#" 
                FROM V$ARCHIVED_LOG val, V$DATABASE vdb 
                WHERE APPLIED = 'YES' AND val.RESETLOGS_CHANGE#=vdb.RESETLOGS_CHANGE#  
                GROUP BY THREAD#
            """
            self.db_cursor.execute(sql)
            rows = self.db_cursor.fetchall()
            for row in rows:
                self.dataguard_slave_status_gauge.labels(row[0], row[1]).set(1)
        except Exception as e:
            logger.error(str(e))


def main():
    start_http_server(HTTP_PORT)
    logger.info("start http server at port:" + str(HTTP_PORT))
    oracledb_collector = OracleCollector()
    while True:
        oracledb_collector.collect()
        time.sleep(SCRAPE_INTERVAL)


if __name__ == '__main__':
    main()
