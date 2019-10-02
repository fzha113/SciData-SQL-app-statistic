# coding: utf-8
import logging
import sys
import MySQLdb

#初始化一个全局的logger
logger = logging.getLogger()
def init_all():
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('./dataCleansingAll.log')

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




class statisticsAll :
    errFile = None
    dbHost = None
    dbName = None
    dbPort = None
    dbUser = None
    dbPassword = None
    conn = None
    cur = None
    dbPortDict = {}
    def callPrcBusAll(self):
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

            #这里如果想取出来中文字段的话,一定要在连接数据库时加上utf8字符的控制
            self.conn = MySQLdb.connect(host = self.dbHost, port = self.dbPort, user = self.dbUser,
                 passwd = self.dbPassword, db = self.dbName, charset='utf8')
            self.cur = self.conn.cursor()

            print ('\033[1;31;40m')
            msg1 = "*************** %s 已开始 ***************" % (value[5])
            print (msg1)

            try:
                self.cur.callproc('prc_bus_scatteredinvest')
                print"************** 散标全量统计完成 ************"
                self.cur.callproc('prc_bus_transactionData')
                print"************** 流水全量统计完成 **************"
                self.cur.callproc('prc_bus_userinfo')
                print"************** 用户全量统计完成 **************"
                self.cur.callproc('prc_bus_financing')
                print"************** 理财全量统计完成 **************"
            except MySQLdb.Error, e:
                msg = "Mysql Error %d: %s " % (e.args[0], e.args[1])
                logger.info(msg)
                print '\033[0m'
            msg2 = "*************** %s 已完成 ***************" % (value[5])
            print (msg2)

            self.cur.close()
            self.conn.close()
        print '\033[0m'


    def getDbPart(self):
        self.dbHost = '10.130.21.25'
        self.dbName = 'test'
        self.dbPort = 3306
        self.dbUser = 'test'
        self.dbPassword = 'test123!'

        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT source_code,host_name,db_name,db_port,db_user,db_passwd,remarks "
                             "FROM sys_platform_db_part")
            data = self.cur.fetchall()
            if data:
                for rec in data:
                    result = "%s,%s,%s,%s,%s,%s" % (rec[0],rec[1],rec[2],rec[3],rec[4],rec[5])
                    p1 = rec[0]
                    p2 = rec[1:]
                    self.dbPortDict[p1]=p2
            self.cur.close()
            self.conn.close()
        except MySQLdb.Error, e:
            msg = "Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.info(msg)






if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    print "============  start   ============"
    aa = statisticsAll()
    aa.getDbPart()
    aa.callPrcBusAll()
    print "============  end   ============"






