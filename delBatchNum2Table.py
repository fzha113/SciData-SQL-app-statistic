# coding: utf-8
import logging
import sys
import MySQLdb
import time

#初始化一个全局的logger
logger = logging.getLogger()
def init_all():
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('/var/log/sci/dataStatistics.log')

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

class delData :
    numTableDict = {}


    def delData3309(self):
        self.dbHost = '127.0.0.1'
        self.dbName = 'test'
        self.dbPort = 3309
        self.dbUser = 'root'
        self.dbPassword = 'admin123'

        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT * "
                             "FROM delTable_TEST20160509005")
            data = self.cur.fetchall()
            batchNum = None
            table = None
            if data:
                for rec in data:
                    result = "%s,%s,%s,%s" % (rec[0], rec[1], rec[2], rec[3])
                    batchNum = str(rec[0])
                    table = str(rec[2])
                    self.cur.execute("delete from "+ table +" where batch_num = '"+ batchNum + "'")
                    print " ======  删除 " + table +"表的 "+ batchNum + "批次,已完成  =========="
            #         self.numTableDict[batchNum] = table
            #
            # for key, value in self.numTableDict.items():
            #     print key
            #     print value

            self.cur.close()
            self.conn.close()
        except MySQLdb.Error, e:
            msg = "getDbPart,Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.info(msg)






    def delData3310(self):
        self.dbHost = '127.0.0.1'
        self.dbName = 'test'
        self.dbPort = 3310
        self.dbUser = 'root'
        self.dbPassword = 'admin123'

        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            self.cur.execute("SELECT * "
                             "FROM delTable_TEST20160509008")
            data = self.cur.fetchall()
            batchNum = None
            table = None
            if data:
                for rec in data:
                    result = "%s,%s,%s,%s" % (rec[0], rec[1], rec[2], rec[3])
                    batchNum = str(rec[0])
                    table = str(rec[2])
                    self.cur.execute("delete from "+ table +" where batch_num = '"+ batchNum + "'")
                    print " ======  删除 " + table +"表的 "+ batchNum + "批次,已完成  =========="
                    # self.numTableDict[batchNum] = table
            #
            # for key, value in self.numTableDict.items():
            #     print key
            #     print value

            self.cur.close()
            self.conn.close()
        except MySQLdb.Error, e:
            msg = "getDbPart,Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.info(msg)

    def removeDelTable2DataBase(self):





if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    print "============  start   ============", '\n\n'
    aa = delData()

    print "============  end   ============"