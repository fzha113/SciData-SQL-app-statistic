# coding: utf-8
import logging
import sys
import MySQLdb
import web

#初始化一个全局的logger
logger = logging.getLogger()
def init_all():
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('./delDataTransit3316.log')

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




class delDataTransit :
    errFile = None
    dbHost = None
    dbName = None
    dbPort = None
    dbUser = None
    dbPassword = None
    conn = None
    cur = None
    dbPortDict = {}
    sysBatchResult = None
    moveResult = None

    def findDelData(self):
        self.dbHost = '10.130.21.25'
        self.dbName = 'test'
        self.dbPort = 3306
        self.dbUser = 'test'
        self.dbPassword ='test123!'
        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            print ("===>    开始收集数据    <===")
            sql = ("SELECT id as batch_num,source_code,"
                     " CASE "
                     " WHEN inf_type = 1 then 'inf_userinfo'"
                     " WHEN inf_type = 2 then 'inf_scatteredinvest'"
                     " WHEN inf_type = 3 then 'inf_financing'"
                     " WHEN inf_type = 4 then CONCAT('inf_transactionData_',SUBSTRING(batch_num,5,4))"
                     " WHEN inf_type = 5 then 'inf_transfer'"
                     " WHEN inf_type = 6 then 'inf_updateStatus'"
                     " WHEN inf_type = 7 then 'inf_expectedreturns'"
                     " ELSE NULL"
                     " END as type,"
                     " is_ok"
                     " FROM `sys_batch`"
                     " WHERE is_ok = 0 AND source_code = 'TEST20160509005'")
            #print sql
            self.cur.execute(sql)
            self.sysBatchResult = self.cur.fetchall()
            self.conn.commit()

            print '将批次表中标记为删除的数据移动到剪切表中'

            self.cur.callproc('delData2delTable')
            self.conn.commit()
            self.cur.close()
            self.conn.close()

            print ("===>    收集数据成功    <===")


        except MySQLdb.Error, e:
            msg = "delDataTransit , Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.info(msg)







    def delDataTransport(self):
        # 用于检查获取各个库信息的字典的异常,如果没有取出来各个库信息这里就会抛出异常退出
        if len(self.sysBatchResult) == 0:
            logger.error('===>    没有要删除的数据    <===')
            raise Exception('===>    没有要删除的数据    <===')

        self.dbHost = '10.130.21.30'
        self.dbName = 'ZRB'
        self.dbPort = 3316
        self.dbUser = 'test'
        self.dbPassword = 'test123!'

        try:
            # 这里如果想取出来中文字段的话,一定要在连接数据库时加上utf8字符的控制
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            print ("===>    开始向库中传输数据    <===")
            #先清除删除表中的数据
            self.cur.execute("truncate table delTable_TEST20160509005")
            self.conn.commit()
            for temp in self.sysBatchResult:
                sql = ("INSERT into delTable_TEST20160509005(`batch_num`, `source_code`, `type`, `is_ok`) "
                       "VALUES ('%s','%s','%s','%s');")
                sql = sql % temp
                print sql
                self.cur.execute(sql)
            self.conn.commit() #必须要提交,不然插入不进库中
            self.cur.close()
            self.conn.close()
            print ("===>    向库中传输数据完成    <===")
        except MySQLdb.Error, e:
            msg = "delDataTransport , Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.error(msg)


    def delData(self):
        self.dbHost = '10.130.21.30'
        self.dbName = 'ZRB'
        self.dbPort = 3316
        self.dbUser = 'test'
        self.dbPassword = 'test123!'
        try:
            self.conn =  web.database(dbn="mysql",db=self.dbName,user=self.dbUser, pw=self.dbPassword, host=self.dbHost,
                                      port=self.dbPort)
            print ("===>    开始删除数据    <===")
            data = self.conn.select('delTable_TEST20160509005')

            if len(data) == 0:
                logger.error('*********delTable_TEST20160509005 is None!******************')
                raise Exception('===>    删除表没有数据    <===')

            if data:
                for rec in data:
                    batchNum = rec['batch_num']
                    table = rec['type']
                    deldata = self.conn.query("select * from "+ table +" where batch_num = '"+ batchNum + "'")
                    for temp in deldata:
                        print '\n\n插入删除表\n\n'
                        if table == 'inf_expectedreturns':
                            self.conn.insert('del_expectedreturns',
                                              version=temp['version'],
                                              source_code=temp['source_code'],
                                              source_product_code=temp['source_product_code'],
                                              source_financing_code=temp['source_financing_code'],
                                              user_balance=temp['user_balance'],
                                              user_rate=temp['user_rate'],
                                              expect_money=temp['expect_money'],
                                              expect_rate=temp['expect_rate'],
                                              expect_date=temp['expect_date'],
                                              user_source=temp['user_source'],
                                              user_idcardhash=temp['user_idcardhash'],
                                              batch_num=temp['batch_num'],
                                              file_id=temp['file_id'],
                                              seq_id=temp['seq_id'],
                                              status=temp['status'],
                                              rcv_time=temp['rcv_time'],
                                              imp_time=temp['imp_time'],
                                              deal_time=temp['deal_time'])
                        if table == 'inf_userinfo':
                            self.conn.insert('del_userinfo',
                                              version=temp['version'],
                                              user_create_time=temp['user_create_time'],
                                              source_code=temp['source_code'],
                                              user_source=temp['user_source'],
                                              user_status=temp['user_status'],
                                              user_type=temp['user_type'],
                                              user_attr=temp['user_attr'],
                                              user_name=temp['user_name'],
                                              user_namehash=temp['user_namehash'],
                                              user_idcard=temp['user_idcard'],
                                              user_idcardhash=temp['user_idcardhash'],
                                              user_uuid=temp['user_uuid'],
                                              user_phone=temp['user_phone'],
                                              user_phonehash=temp['user_phonehash'],
                                              user_lawperson=temp['user_lawperson'],
                                              user_fund=temp['user_fund'],
                                              user_province=temp['user_province'],
                                              user_address=temp['user_address'],
                                              register_date=temp['register_date'],
                                              user_mail=temp['user_mail'],
                                              batch_num=temp['batch_num'],
                                              file_id=temp['file_id'],
                                              seq_id=temp['seq_id'],
                                              status=temp['status'],
                                              rcv_time=temp['rcv_time'],
                                              imp_time=temp['imp_time'],
                                              deal_time=temp['deal_time'])
                        if table == 'inf_financing':
                            self.conn.insert('del_financing',
                                             version=temp['version'],
                                             borrowamt=temp['borrowamt'],
                                             term_type=temp['term_type'],
                                             term=temp['term'],
                                             begindate=temp['begindate'],
                                             enddate=temp['enddate'],
                                             financing_start_time=temp['financing_start_time'],
                                             product_reg_type=temp['product_reg_type'],
                                             product_name=temp['product_name'],
                                             product_mark=temp['product_mark'],
                                             source_code=temp['source_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             plan_raise_amount=temp['plan_raise_amount'],
                                             rate=temp['rate'],
                                             isshow=temp['isshow'],
                                             remark=temp['remark'],
                                             amount_limmts=temp['amount_limmts'],
                                             amount_limmtl=temp['amount_limmtl'],
                                             red_rate=temp['red_rate'],
                                             source_product_url=temp['source_product_url'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_scatteredinvest':
                            self.conn.insert('del_scatteredinvest',
                                             version=temp['version'],
                                             product_start_time=temp['product_start_time'],
                                             product_reg_type=temp['product_reg_type'],
                                             product_name=temp['product_name'],
                                             product_mark=temp['product_mark'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             loan_use=temp['loan_use'],
                                             loan_describe=temp['loan_describe'],
                                             amount=temp['amount'],
                                             rate=temp['rate'],
                                             term_type=temp['term_type'],
                                             term=temp['term'],
                                             pay_type=temp['pay_type'],
                                             service_cost=temp['service_cost'],
                                             risk_margin=temp['risk_margin'],
                                             loan_type=temp['loan_type'],
                                             loan_credit_rating=temp['loan_credit_rating'],
                                             security_info=temp['security_info'],
                                             collateral_desc=temp['collateral_desc'],
                                             collateral_info=temp['collateral_info'],
                                             overdue_limmit=temp['overdue_limmit'],
                                             bad_debt_limmit=temp['bad_debt_limmit'],
                                             amount_limmts=temp['amount_limmts'],
                                             amount_limmtl=temp['amount_limmtl'],
                                             allow_transfer=temp['allow_transfer'],
                                             close_limmit=temp['close_limmit'],
                                             security_type=temp['security_type'],
                                             project_source=temp['project_source'],
                                             source_product_url=temp['source_product_url'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transactionData_2013':
                            self.conn.insert('del_transactionData_2013',
                                             version=temp['version'],
                                             trans_time=temp['trans_time'],
                                             trans_id=temp['trans_id'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             trans_type=temp['trans_type'],
                                             trans_type_dec=temp['trans_type_dec'],
                                             trans_money=temp['trans_money'],
                                             trans_date=temp['trans_date'],
                                             trans_payment=temp['trans_payment'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             trans_bank=temp['trans_bank'],
                                             trans_account=temp['trans_account'],
                                             trans_source_peer=temp['trans_source_peer'],
                                             trans_bank_peer=temp['trans_bank_peer'],
                                             trans_account_peer=temp['trans_account_peer'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transactionData_2014':
                            self.conn.insert('del_transactionData_2014',
                                             version=temp['version'],
                                             trans_time=temp['trans_time'],
                                             trans_id=temp['trans_id'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             trans_type=temp['trans_type'],
                                             trans_type_dec=temp['trans_type_dec'],
                                             trans_money=temp['trans_money'],
                                             trans_date=temp['trans_date'],
                                             trans_payment=temp['trans_payment'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             trans_bank=temp['trans_bank'],
                                             trans_account=temp['trans_account'],
                                             trans_source_peer=temp['trans_source_peer'],
                                             trans_bank_peer=temp['trans_bank_peer'],
                                             trans_account_peer=temp['trans_account_peer'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transactionData_2015':
                            self.conn.insert('del_transactionData_2015',
                                             version=temp['version'],
                                             trans_time=temp['trans_time'],
                                             trans_id=temp['trans_id'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             trans_type=temp['trans_type'],
                                             trans_type_dec=temp['trans_type_dec'],
                                             trans_money=temp['trans_money'],
                                             trans_date=temp['trans_date'],
                                             trans_payment=temp['trans_payment'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             trans_bank=temp['trans_bank'],
                                             trans_account=temp['trans_account'],
                                             trans_source_peer=temp['trans_source_peer'],
                                             trans_bank_peer=temp['trans_bank_peer'],
                                             trans_account_peer=temp['trans_account_peer'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transactionData_2016':
                            self.conn.insert('del_transactionData_2016',
                                             version=temp['version'],
                                             trans_time=temp['trans_time'],
                                             trans_id=temp['trans_id'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             trans_type=temp['trans_type'],
                                             trans_type_dec=temp['trans_type_dec'],
                                             trans_money=temp['trans_money'],
                                             trans_date=temp['trans_date'],
                                             trans_payment=temp['trans_payment'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             trans_bank=temp['trans_bank'],
                                             trans_account=temp['trans_account'],
                                             trans_source_peer=temp['trans_source_peer'],
                                             trans_bank_peer=temp['trans_bank_peer'],
                                             trans_account_peer=temp['trans_account_peer'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transactionData_2017':
                            self.conn.insert('del_transactionData_2017',
                                             version=temp['version'],
                                             trans_time=temp['trans_time'],
                                             trans_id=temp['trans_id'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             trans_type=temp['trans_type'],
                                             trans_type_dec=temp['trans_type_dec'],
                                             trans_money=temp['trans_money'],
                                             trans_date=temp['trans_date'],
                                             trans_payment=temp['trans_payment'],
                                             user_source=temp['user_source'],
                                             user_idcardhash=temp['user_idcardhash'],
                                             trans_bank=temp['trans_bank'],
                                             trans_account=temp['trans_account'],
                                             trans_source_peer=temp['trans_source_peer'],
                                             trans_bank_peer=temp['trans_bank_peer'],
                                             trans_account_peer=temp['trans_account_peer'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                        if table == 'inf_transfer':
                            self.conn.insert('del_transfer',
                                             version=['version'],
                                             transfer_start_time=['transfer_start_time'],
                                             product_reg_type=['product_reg_type'],
                                             product_name=['product_name'],
                                             product_mark=['product_mark'],
                                             source_code=['source_code'],
                                             source_product_code=['source_product_code'],
                                             source_financing_code=['source_financing_code'],
                                             transfer_name=['transfer_name'],
                                             hold_time=['hold_time'],
                                             overplus_time=['overplus_time'],
                                             amt=['amt'],
                                             transfer_rate=['transfer_rate'],
                                             transfer_fee=['transfer_fee'],
                                             remark=['remark'],
                                             source_product_url=['source_product_url'],
                                             batch_num=['batch_num'],
                                             file_id=['file_id'],
                                             seq_id=['seq_id'],
                                             status=['status'],
                                             rcv_time=['rcv_time'],
                                             imp_time=['imp_time'],
                                             deal_time=['deal_time'])
                        if table == 'inf_updateStatus':
                            self.conn.insert('del_updateStatus',
                                             version=temp['version'],
                                             source_code=temp['source_code'],
                                             source_product_code=temp['source_product_code'],
                                             source_financing_code=temp['source_financing_code'],
                                             product_status=temp['product_status'],
                                             produc_status_desc=temp['produc_status_desc'],
                                             product_date=temp['product_date'],
                                             batch_num=temp['batch_num'],
                                             file_id=temp['file_id'],
                                             seq_id=temp['seq_id'],
                                             status=temp['status'],
                                             rcv_time=temp['rcv_time'],
                                             imp_time=temp['imp_time'],
                                             deal_time=temp['deal_time'])
                    print '\n\n\n\n将inf表中数据删除\n\n\n\n'
                    #插入需要一条一条插入,删除是一批一批删除
                    self.conn.query("delete from " + table + " where batch_num = '" + batchNum + "'")
        except MySQLdb.Error, e:
            msg = "delData , Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.error(msg)



    #调用存储过程来insert和delete数据
    def delDataProc(self):
        self.dbHost = '10.130.21.30'
        self.dbName = 'ZRB'
        self.dbPort = 3316
        self.dbUser = 'test'
        self.dbPassword = 'test123!'
        try:
            self.conn = MySQLdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser,
                                        passwd=self.dbPassword, db=self.dbName, charset='utf8')
            self.cur = self.conn.cursor()
            print ("===>    开始删除数据    <===")
            self.cur.callproc('insertAndDeleteData')
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            print ("===>    删除数据完成    <===")
        except MySQLdb.Error, e:
            msg = "delDataProc , Mysql Error %d: %s " % (e.args[0], e.args[1])
            logger.error(msg)






if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    print "============  start   ============", '\n\n'
    aa = delDataTransit()

    aa.findDelData()
    aa.delDataTransport()
    aa.delDataProc()

    print "\n\n============  end   ============"