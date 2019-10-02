# coding: utf-8
import logging
import sys
import MySQLdb
import time

#初始化一个全局的logger
logger = logging.getLogger()
def init_all():
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('./statisticsToJM.log')

    # 再创建一个handler，用于输出到控制台
    ch = logging.StreamHandler()

    # 定义handler的输出格式formatter
    formatter = logging.Formatter('%(asctime)s-%(lineno)d-%(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)

init_all()

class statisticsToJM :
    """
    表的对应关系如下:
        p2_Result = ptyyqkylb   26
        p3_Result = fxfxylb     13
        p5_Result = pt_hyjy_tj  5
        p15_Result =ptzjlmx_02  11
        p16_Result =ptzjl_tj    5
        p17_Result =ptbdqx_tj   4
        p18_Result =ptbdje_tj   4
        p19_Result =ptjye_ztj   4
        p20_Result =ptxxltj_02  11
    """
    errFile = None
    dbHost = None
    dbName = None
    dbPort = None
    dbUser = None
    dbPassword = None
    conn = None
    cur = None
    dbPortDict = {}
    p2_Result = None
    p3_Result = None
    p5_Result = None
    p15_Result = None
    p16_Result = None
    p17_Result = None
    p18_Result = None
    p19_Result = None
    p20_Result = None

    is_insert = False

    def __init__(self):
        pass

    def findAll(self):
        # 用于检查获取各个库信息的字典的异常,如果没有取出来各个库信息这里就会抛出异常退出
        if len(self.dbPortDict) == 0:
            logger.error('*********dbPortDict is None!******************')
            raise Exception('dbPortDict cannot be None!')
            #将各个库中信息通过每次遍历形成连接并调用数据库过程
        for key,value in self.dbPortDict.items():
            self.dbHost = value[0]
            self.dbName = value[1]
            self.dbPort = value[2]
            self.dbUser = value[3]
            self.dbPassword = value[4]

            self.p2_Result = None
            self.p3_Result = None
            self.p5_Result = None
            self.p15_Result = None
            self.p16_Result = None
            self.p17_Result = None
            self.p18_Result = None
            self.p19_Result = None
            self.p20_Result = None

            #这里如果想取出来中文字段的话,一定要在连接数据库时加上utf8字符的控制
            self.conn = MySQLdb.connect(host = self.dbHost, port = self.dbPort, user = self.dbUser,
                 passwd = self.dbPassword, db = self.dbName, charset='utf8')
            self.cur = self.conn.cursor()

            msg1 = "*************** %s:%d:%s begining! ***************" % (value[5], value[2], self.dbName)
            logger.info(msg1)

            try:
                #p_2
                sql = ('select * from ptyyqkylb '
                       'WHERE tjsj > left(NOW(),10) ORDER BY tjsj DESC LIMIT 1;')
                self.cur.execute(sql)
                self.p2_Result = self.cur.fetchall()
                if not self.p2_Result:
                    logger.warning("--ptyyqkylb has not data")

                #p_3
                sql = ('SELECT * FROM fxfxylb '
                       'where tjsj > left(NOW(),10) ORDER BY tjsj DESC LIMIT 1')
                self.cur.execute(sql)
                self.p3_Result = self.cur.fetchall()
                if not self.p3_Result:
                    logger.warning("--fxfxylb has not data")

                #p_5
                #取每天的前1000条
                sql = ('select * from pt_hyjy_tj '
                       'WHERE sj > left(NOW(),10) ORDER BY sj DESC limit 1000;')
                self.cur.execute(sql)
                self.p5_Result = self.cur.fetchall()
                if not self.p5_Result:
                    logger.warning("--pt_hyjy_tj has not data")

                #p_15
                #这里取每天前一天的前1000条
                sql = ('SELECT * FROM ptzjlmx_02 '
                       'WHERE sj > date_sub(curdate(),interval 1 day) ORDER BY sj DESC limit 1000;')
                self.cur.execute(sql)
                self.p15_Result = self.cur.fetchall()
                if not self.p15_Result:
                    logger.warning("--ptzjlmx_02 has not data")

                #p_16
                sql = ('select * from ptzjl_tj '
                       'WHERE tjsj > left(NOW(),10);')
                self.cur.execute(sql)
                self.p16_Result = self.cur.fetchall()
                if not self.p16_Result:
                    logger.warning("--ptzjl_tj has not data")

                #p_17
                sql = ('select * from ptbdqx_tj '
                       'WHERE tjsj > left(NOW(),10);')
                self.cur.execute(sql)
                self.p17_Result = self.cur.fetchall()
                if not self.p17_Result:
                    logger.warning("--ptbdqx_tj has not data")

                # p_18
                sql = ('select * from ptbdje_tj '
                      'WHERE tjsj > left(NOW(),10);')
                self.cur.execute(sql)
                self.p18_Result = self.cur.fetchall()
                if not self.p18_Result:
                    logger.warning("--ptbdje_tj has not data")

                # p_19
                # every week?
                sql = 'select * from ptjye_ztj WHERE tjsj > left(NOW(),10);'
                self.cur.execute(sql)
                self.p19_Result = self.cur.fetchall()
                if not self.p19_Result:
                    logger.warning("--ptjye_ztj has not data")

                # p_20
                # 这里取每天前一天的前1000条
                sql = 'SELECT * FROM `ptxxltj_02` WHERE kbsj > date_sub(curdate(),interval 1 day)' \
                      ' ORDER BY kbsj DESC limit 1000;'
                self.cur.execute(sql)
                self.p20_Result = self.cur.fetchall()
                if not self.p20_Result:
                    logger.warning("--ptxxltj_02 has not data")

                #在每一次循环取数据后,调用插入界面库的方法,将各个resultinsert到界面库中
                self.insertUiDb()

            except MySQLdb.Error, e:
                msg = "callPrcBusAll,Mysql Error %d: %s " % (e.args[0], e.args[1])
                logger.error(msg)

            msg2 = "*************** %s 已完成 ***************" % (value[5])
            logger.info(msg2)

            self.cur.close()
            self.conn.close()

    def getDbPart(self):
        self.dbHost = '10.130.21.25'
        self.dbName = 'test'
        self.dbPort = 3306
        self.dbUser = 'test'
        self.dbPassword = 'test123!'

        if self.is_insert is True:
            logger.info("is_insert is true")
        else:
            logger.info("is_insert is False")

        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT source_code,host_name,db_name,db_port,db_user,db_passwd,remarks "
                             "FROM sys_platform_db_part")
            data = self.cur.fetchall()
            if data:
                for rec in data:
                    p1 = rec[0]
                    p2 = rec[1:]
                    self.dbPortDict[p1]=p2
            self.cur.close()
            self.conn.close()
        except MySQLdb.Error, e:
            msg = "getDbPart,Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.info(msg)

    def insertUiDb(self):
        if self.p2_Result is None:
            logger.error('*********p2_Result is None!******************')
            raise Exception('The statistics result cannot be None!')

        conn = MySQLdb.connect(host='10.130.21.150', user='jiemian',
                               passwd='jiemian12345', db='funds_task',port=3306, charset='utf8')
        cur = conn.cursor()
        try:
            print  "insert into p2"
            for temp in self.p2_Result:
                sql = ("INSERT into ptyyqkylb "
                       "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
                       "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p3"
            for temp in self.p3_Result:
                sql = ('INSERT into fxfxylb '
                       "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p5"
            for temp in self.p5_Result:
                sql = ('INSERT into pt_hyjy_tj '
                      "VALUES ('%s','%s','%s','%s','%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p15"
            for temp in self.p15_Result:
                sql = ('INSERT into ptzjlmx_02 '
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p16"
            for temp in self.p16_Result:
                sql = ('INSERT into ptzjl_tj '
                       "VALUES ('%s','%s','%s','%s','%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p17"
            for temp in self.p17_Result:
                sql = ('INSERT into ptbdqx_tj '
                       "VALUES ('%s','%s','%s','%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p18"
            for temp in self.p18_Result:
                sql = ('INSERT into ptbdje_tj '
                      "VALUES ('%s','%s','%s','%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p19"
            for temp in self.p19_Result:
                sql = ('INSERT into ptjye_ztj '
                       "VALUES ('%s','%s','%s','%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        try:
            print  "insert into p20"
            for temp in self.p20_Result:
                sql = ('INSERT into ptxxltj_02 '
                        "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s',\"%s\",'%s');")
                sql = sql % temp
                if self.is_insert:
                    cur.execute(sql)
                else:
                    print "sql is:", sql
                    break
        except MySQLdb.Error, e:
            msg = "Mysql Error %d:%s:%s " % (e.args[0], e.args[1], sql)
            logger.error(msg)

        #程序控制的数据库语句需要自己commit
        conn.commit()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    print "============  start   ============",'\n\n'
    toJM = statisticsToJM()
    toJM.is_insert = True
    toJM.getDbPart()
    toJM.findAll()
    toJM.insertUiDb()
    print "============  end   ============"






