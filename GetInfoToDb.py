#_*_encoding:utf-8_*_
import datetime
import time
import urllib2
import os
import sys
import MySQLdb
from datetime import datetime
from datetime import date
from datetime import timedelta
import multiprocessing

class BitError(Exception): pass

class LinkToDb:
    # 查询数据库
    dbConn = None
    dbCur = None
    errFile = None
    isInsertDb = False
    exchIds = []
    exchanges = []
    info = []
    beginTime = None
    endTime = None
    span = 1
    zxt = None
    lxph = None
    tradePrice = 1

    def __init__(self, ipPath = "./error.txt"):
        if os.path.isfile(ipPath):
            self.errFile = ipPath
        elif os.path.isdir(ipPath):
            raise BitError("the file " + ipPath + " is a directory, not a file")

    def __del__(self):
        pass

    def ErrorToFile(self, msg):
        file_object = open(self.errFile, "a+")
        file_object.write(msg + "\n")
        file_object.close()

    def initDb(self):
        try:
            self.dbConn = MySQLdb.connect(host='202.108.211.55',user='root',passwd='cncert.cncert@nf',db='test', port=3306, charset='utf8')
            self.dbCur = self.dbConn.cursor()
        except MySQLdb.Error, e:
            msg = "Mysql Error %d: %s " % (e.args[0], e.args[1])
            self.ErrorToFile(msg)
            raise

    def reinit_db(self):
        try:
            self.dbConn = MySQLdb.connect(host='10.130.21.150',user='jiemian',passwd='jiemian12345',db='funds_task', port=3306, charset='utf8')
            self.dbCur = self.dbConn.cursor()
        except MySQLdb.Error, e:
            msg = "Mysql Error %d: %s " % (e.args[0], e.args[1])
            self.ErrorToFile(msg)
            raise

    def close(self):
        if self.dbCur is not None:
            self.dbCur.close()
        if self.dbConn is not None:
            self.dbConn.close()
        self.ErrorToFile("--------------- closed --------------------")

    def MultilOpenUrl(self, url):
        """
            查询url, 并返回对应的结果json
            1. 404, 直接退出
            2. ssl, 重试三次, 若不行, 则记录
            3. timeout, 重试三次, 若不行记录
            4. 其它, 重试三次, 不行记录
            返回值, 若Json, 成功; 若false, 则失败, 要记录等下次; 若None, 无数据(404)
        """
        times = 1
        while True:
            try:
                rep = urllib2.urlopen(url, timeout=10)
                data_str = rep.read()
                # chain_json = json.loads(data_str)
                # return chain_json
                return data_str
            except urllib2.HTTPError, e:
                if e.code == 404:
                    msg = "HTTPError is %d:%s, url:%s" % (e.code, e.reason, url)
                    print msg
                    self.ErrorToFile(msg)
                    return None
                elif e.code == 500:
                    msg = "HTTPError is %d:%s, url:%s" % (e.code, e.reason, url)
                    print msg
                    self.ErrorToFile(msg)
                    return None
            # except ssl.SSLError, e:
            #    print "SSLError for %s:%s" % (e.reason, url)
            except Exception, e:
                if times % 5 == 0:
                    msg = "Url Error (%d) times, errors: %s, url: %s" % (times, e, url)
                    print msg
                    self.ErrorToFile(msg)
                    if times >= 200:
                        times = 0
                times += 1
                sleepT = 6 * times
                time.sleep(sleepT)

    def get_trade(self):
        """
        :return: none
        """
        self.exchanges = []
        url = "https://www.okcoin.cn/"
        date = self.MultilOpenUrl(url)
        ht = etree.HTML(date)
        xpath_str = ".//*[@id='dataTradePriceBtc']"
        tds = ht.xpath(xpath_str)

        for tmptds in tds:
            tds = tmptds.text
            if tds is not None:
                tds = tds.replace(',', '')
                self.tradePrice = float(tds)
                print "trade price is :", self.tradePrice
                return
        self.x = 1

    def GetWalletByExchange(self, exchange, idx):
        url = "https://www.walletexplorer.com/wallet/" + exchange
        pos = 1
        maxPages = 1
        self.info = []

        msg = "----- will get wallet of %s:%s ----" % (idx, exchange)
        print msg
        self.ErrorToFile(msg)

        while True:
            print "--get pages in ", pos
            # self.ErrorToFile("--get pages in " + str(pos))
            print "url is :", url
            isExpiry = False
            if pos == 1:
                data = self.MultilOpenUrl(url)
                if data == None:
                    continue
                ht = etree.HTML(data)
                pages = ht.xpath("//div[@class='paging'][1]/text()")[0]
                print "page is :", pages
                pages = pages.split(' ')[4]
                print "page is :", pages
                maxPages = int(pages)
                newurl = url + "?format=csv"
                msg = "max pages is: %s" % (maxPages)
                print msg
                self.ErrorToFile(msg)
            else:
                newurl = url + "?page=" + str(pos) + "&format=csv"
            print "new url is ", newurl
            data = self.MultilOpenUrl(newurl)
            lines = data.split("\r\n")
            lineStep = 0
            if len(lines) <= 0:
                msg = "has no lines in url: " + newurl
                print msg
                self.ErrorToFile(msg)
            for line in lines:
                lineStep += 1
                if lineStep < 3 or len(line) <= 0:
                    continue
                item = line.split(",")
                if len(item) != 7:
                    msg = "%s:%d line is has error, line is: %s" % (exchange, pos, line)
                    print msg
                    self.ErrorToFile(msg)
                    continue
                item[1] = item[1].replace('"', '')
                item[4] = item[4].replace('"', '')
                item[6] = item[6].replace('"', '')

                date = datetime.strptime(item[0], "%Y-%m-%d %H:%M:%S")
                # beginTime 01-01   endTime 12-01
                if date >= self.endTime:
                    # print "is not reach endTime"
                    continue
                if date < self.beginTime:
                    # print "isExpiry is True, date is: ", item[0]
                    isExpiry = True
                    break

                isFind = True
                for exch in self.exchanges:
                    exch = str(exch)
                    if len(item[1]) > 0:
                        if item[1].find(exch) != -1:
                            item[1] = exch
                            break
                    elif len(item[4]) > 0:
                        if item[4].find(exch) != -1:
                            item[4] = exch
                            break
                        if item[4] == u"(fee)":
                            item[4] = u"fee"
                            break
                    else:
                        isFind = False
                        break
                # print "get Date is:", item
                if isFind is True:
                    self.info.append(item)
            pos += 1
            if pos % 50 == 0:
                msg = "will insert wallet to db, " + str(pos)
                print msg
                self.ErrorToFile(msg)
                self.InsertInfoToWallet(idx)
            if pos > maxPages or isExpiry is True:
                msg = "will insert wallet to db by max Pages or isExpiry and exist, curpos is: " + str(pos)
                print msg
                self.ErrorToFile(msg)
                self.InsertInfoToWallet(idx)
                break

    def InsertInfoToWallet(self, exchange_id):
        """
            插入数据到表bitWallet中去, 数据是GetWalletByExchange中获取的self.info
        """
        while True:
            try:
                cnt = 0
                for item in self.info:
                    cnt += 1
                    self.dbCur.execute("insert into bitWalletNewOk values(" + str(exchange_id) + ",%s,%s,%s,%s,%s,%s,%s)", item)
                    if cnt % 1000 == 0:
                        self.dbConn.commit()
                self.dbConn.commit()
                print "will commint"
                self.ErrorToFile("will commint")
                break
            except MySQLdb.Error,e:
                msg = "In DoInsertIpByHash, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()
        self.info = []

    def GetAllExchanges(self):
        while True:
            try:
                num = self.dbCur.execute('SELECT id, exchange from bitExchange order by id')
                while True:
                    aa = self.dbCur.fetchone()
                    if aa is None:
                        break
                    self.exchIds.append(aa[0])
                    self.exchanges.append(aa[1])
                print "finish get all exchanges: ", self.exchanges
                self.ErrorToFile("finish get all exchanges")
                break
            except MySQLdb.Error,e:
                msg = "In DoInsertIpByHash, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()

    def InsertToExchange(self, idtype):
        """
            插入数据到表bitExchange中去, 数据是GetWalletByExchange中获取的self.info
        """
        while True:
            try:
                cnt = 0
                for item in self.exchanges:
                    sql = "insert into bitExchange values(" + str(idtype*100+cnt) + ",%s, 0, " + str(idtype) + ") "
                    # print "sql is " + sql + item
                    cnt += 1
                    self.dbCur.execute(sql, item);
                self.dbConn.commit()
                msg = "execute sql(%s) success, and will commit" % sql
                print msg
                self.ErrorToFile(msg)
                break
            except MySQLdb.Error, e:
                msg = "In DoInsertIpByHash, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()

    def get_max_day(self, tbl):
        """
        获取最大的时间, 然后, 转化为span的变量
        :return:
        """
        while True:
            try:
                sql = "SELECT max(date) from %s ;" % tbl
                get_time = ""
                get_num = self.dbCur.execute(sql)
                if get_num > 1 or get_num == 0:
                    get_time = "2015-12-31 11:00:00"
                else:
                    aa = self.dbCur.fetchone()
                    if aa[0] is None:
                        get_time = "2015-12-31 11:00:00"
                    else:
                        get_time = str(aa[0])
                span = self.getspan(get_time)
                if span > 1:
                    span -= 1
                self.span = span
                self.dbConn.commit()
                msg = "execute sql(%s) success, and time:span is (%s):(%d)" % (sql, get_time, span)
                print msg
                self.ErrorToFile(msg)
                break
            except MySQLdb.Error, e:
                msg = "In get_max_day, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()

    def DoGetOneday(self):
        """
            把数据从bitWalletNew到bitDay, 这样子就可以统计了;
        """
        while True:
            try:
                sql = "call getBitcoinExchange(%d); commit;" % self.span
                self.dbCur.callproc("getBitcoinExchange", (self.span,))
                # self.dbCur.execute(sql)
                self.dbConn.commit()
                msg = "execute sql(%s) success, and will commit" % sql
                print msg
                self.ErrorToFile(msg)
                break
            except MySQLdb.Error, e:
                msg = "In DoGetOneday, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()

    def get_data_to_result(self):   #self指的是类实例对象本身(这里指的是linkToDb的对象)对于对象自身的引用
        #在def语句后面(以及在模块或者类的开头写下字符串,它就会作为函数的一部分进行存储,
        # 使用 >>>help( get_data_to_result)可以在解释器中得到文档字符串)
        #self参数事实上正式方法和函数的区别.方法(更专业一点可以成为绑定方法)将它们的第一个参数绑定到所属的实例上,
        # 因此您无需显示提供该参数绑定到所属的实例上
        """
            获取数据到该类的变量中; 从bitDay查询数据库结果到zxt与lxph这两个变量
        :return:
        """
        while True:
            try:
                sql = ("SELECT bd.type, bd.date, sum(bd.amount), sum(bd.sum) "
                        "FROM bitDay AS bd "
                        "WHERE bd.country NOT IN ('UNKNOW', '未知') "
                        "GROUP BY bd.type, bd.date "
                        "ORDER BY bd.type, bd.date;")
                self.dbCur.execute(sql) #cursor用来执行命令的方法
                self.zxt = self.dbCur.fetchall() #fetchall()用来接收全部的返回结果行
                print "execut sql (%s) success" % sql
                sql = ("SELECT bd.type, bd.country, sum(bd.amount), sum(bd.sum), sum(bd.amount*%f) "
                       "FROM bitDay AS bd "
                       "WHERE bd.country NOT IN ('UNKNOW', '未知') "
                       "GROUP BY bd.type, bd.country;")
                a = sql % self.tradePrice
                sql = a
                self.dbCur.execute(sql)
                self.lxph = self.dbCur.fetchall()
                msg = "execut sql (%s) success" % sql
                print msg
                self.ErrorToFile(msg)
                self.dbConn.commit()
                break
            except MySQLdb.Error, e:
                msg = "In get_data_to_result, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.initDb()

    def insert_ui(self):
        """
            把zxt与lxph的数据放入到界面的数据库中;
        :return:
        """
        while True:
            try:
                # -------------------------------
                if self.zxt is None:
                    msg = "the table btb_zxt has no data to insert"
                    print msg
                    self.ErrorToFile(msg)
                    break
                sql = "start transaction;"
                self.dbCur.execute(sql)
                sql = "TRUNCATE btb_zxt; "
                self.dbCur.execute(sql)
                for tmpitem in self.zxt:
                    # print tmpitem
                    sql = "insert into btb_zxt (lx,tjsj,sl,zl) values(%s,%s,%s,%s); "
                    self.dbCur.execute(sql, tmpitem)
                self.dbCur.execute("commit;")
                # -------------------------------
                if self.lxph is None:
                    msg = "the table btb_lxph has no data to insert"
                    print msg
                    self.ErrorToFile(msg)
                    break
                sql = "start transaction; "
                self.dbCur.execute(sql)
                sql = "TRUNCATE btb_lxph; "
                self.dbCur.execute(sql)
                for tmpitem in self.lxph:
                    sql = "insert into btb_lxph (lx,gj,sl,bs,je) values(%s,%s,%s,%s,%s); "
                    # print tmpitem
                    self.dbCur.execute(sql, tmpitem)
                self.dbCur.execute("commit;")

                msg = "insert_ui success"
                print msg
                self.ErrorToFile(msg)
                # -------------------------------
                break
            except MySQLdb.Error, e:
                msg = "In insert_ui, Mysql Error %d: %s " % (e.args[0], e.args[1])
                print msg
                print "will sleep 60s and restart database"
                self.ErrorToFile(msg)
                time.sleep(60)
                self.reinit_db()

    def getspan(self, give_time):
        try:
            len_str = len(give_time)
            if len_str > 10:
                a1 = datetime.strptime(give_time, "%Y-%m-%d %H:%M:%S")
            else:
                a1 = datetime.strptime(give_time, "%Y-%m-%d")
            a11 = date(a1.year, a1.month, a1.day)
            a2 = datetime.now()
            a21 = date(a2.year, a2.month, a2.day)
            if a21 > a11:
                span = (a21 - a11).days
                return span
        except ValueError, e:
            msg = "getspan error: ", e
            print msg
            self.ErrorToFile(msg)
        return 0

def DoWhat(mypos, step, span, start_id, end_id):
    """
    在多进程时, 运行使用
    :param mypos: 一般指的是传进来的序号, 从0开始, 当 x % step == mypos时, 可以执行, 否则跳过
    :param step: 步数
    :param span: 时间的跨度, 若<=2则自动变为3, 取的是从昨天到span-2的天数的数据
    :param start_id: 开始, [start_id, end_id)
    :param end_id: 结束
    :return: 无
    """

    if span <= 1:
        msg = "the span is (%d), means no need to get more day" % span
        print msg
        return

    # 初始化相关
    aa = LinkToDb()
    aa.initDb()
    aa.isInsertDb = True

    # 定义时间
    # [beginTime, endTime)
    now = datetime.now()
    before1 = now - timedelta( days=1 )
    before2 = now - timedelta(days=span)
    aa.endTime = datetime(before1.year, before1.month, before1.day)
    aa.beginTime = datetime(before2.year, before2.month, before2.day)

    msg = "[beginTime, endTime)=[%s, %s) in process %s" % (aa.beginTime, aa.endTime, mypos)
    print msg
    aa.ErrorToFile(msg)

    # 获取所有的exchange
    aa.GetAllExchanges()
    # [startIdPos, endIdPos)

    idPos = 0
    for oneExch in aa.exchanges:
        real_id = aa.exchIds[idPos]
        if real_id < start_id:
            pass
        elif real_id > end_id:
            break
        elif real_id % step == mypos:
            msg = "will do id (%d) for exchange(%s) in process (%d)" % (real_id, oneExch, mypos)
            print msg
            aa.ErrorToFile(msg)
            aa.GetWalletByExchange(oneExch, real_id)
        idPos += 1
    aa.close()

if __name__ == '__main__':
    #在linux下对于utf-8支持不全,需要设置下,在字符串中对于中文字符会报错(没有%u),在python3.0之后不会有这个问题了
    reload(sys)
    sys.setdefaultencoding('utf-8')

    myaa = LinkToDb()
    myaa.initDb()
    myaa.isInsertDb = True

    # # 0. 获取汇率
    # myaa.get_trade()

    # # 1. 读取bitWalletNewOk的最大时间, 获取基础库数据的更新
    # myaa.get_max_day("bitWalletNewOk")
    # myspan = myaa.span
    # mystep = 8
    # myresult = []
    # pool = multiprocessing.Pool(processes = mystep)
    # for i in range(mystep):
    #     print "will do %d" % (i)
    #     myresult.append(pool.apply_async(DoWhat, (i, mystep, myspan, 0, 600)))
    # pool.close()
    # pool.join()
    #
    # for tmp_ret in myresult:
    #     print tmp_ret.get()

    # 2. 从bitDay中获取数据到成员变量中lxph/zxt
    # myaa.get_max_day("bitDay")
    # myaa.DoGetOneday()
    myaa.get_data_to_result()

    # 3. 把lxph/zxt的数据放入到界面库中(首先要关掉旧的连接, 然后要连接到界面库, 然后添加数据到界面库中
    myaa.close()
    myaa.reinit_db()
    myaa.insert_ui()

    print "--------------- end --------------------"
