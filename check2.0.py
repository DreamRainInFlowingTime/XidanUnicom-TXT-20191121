import requests
import re
import os
from lxml import etree
#import lxml
import time
from enum import Enum, unique
import sys
from datetime import datetime
import time
import pathlib


class ClusterLocation(Enum):
    LANGFANG = 5
    HUHE = 6

headers = {
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
         'Connection': 'close'
        #'Referer': 'http://www.hangyewz.com/'
    }

#CLUSTER_URL_HUHE=[ClusterLocation.HUHE,'http://10.177.18.216:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.217:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.218:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.219:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.220:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.39:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.40:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.41:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.42:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.43:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.82:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.83:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.84:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.85:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.86:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.122:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.4:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.5:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.44:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.45:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.98:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.103:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.6:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.7:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.49:4044/jobs?page=1&items=1000&status={0}']
CLUSTER_URL_HUHE=[ClusterLocation.HUHE,'http://10.177.18.216:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.217:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.218:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.219:4044/jobs?page=1&items=1000&status={0}','http://10.177.18.220:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.39:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.40:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.41:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.42:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.43:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.82:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.83:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.84:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.85:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.86:4044/jobs?page=1&items=1000&status={0}','http://10.177.19.122:4044/jobs?page=1&items=1000&status={0}']

#只能根据ip来定位serverNum
serverNumDist = {'10.177.18.216':'1','10.177.18.217':'2','10.177.18.218':'3','10.177.18.219':'4','10.177.18.220':'5','10.177.19.39':'6','10.177.19.40':'7','10.177.19.41':'8','10.177.19.42':'9','10.177.19.43':'10','10.177.19.82':'11','10.177.19.83':'12','10.177.19.84':'13','10.177.19.85':'14','10.177.19.86':'15','10.177.19.122':'16','10.177.19.4':'20','10.177.19.5':'21','10.177.19.44':'22','10.177.19.45':'23','10.177.19.98':'24','10.177.19.103':'25','10.177.19.6':'26','10.177.19.7':'27','10.177.19.49':'28','10.177.19.50':'29','10.177.19.106':'30','10.177.19.107':'31','10.177.19.108':'32','10.177.19.51':'33','10.177.19.55':'34','10.177.19.8':'35','10.177.19.10':'36','10.177.19.16':'37','10.177.19.56':'38','10.177.19.57':'39','10.177.19.58':'40','10.177.19.109':'41','10.177.19.111':'42','10.177.19.110':'43','10.177.19.21':'44','10.162.235.40':'1','10.162.235.41':'2','10.162.235.43':'3'}


CLUSTER_URL_LANGFANG=[ClusterLocation.LANGFANG,'http://10.162.235.40:4044/jobs?page=1&items=1000&status={0}','http://10.162.235.41:4044/jobs?page=1&items=1000&status={0}','http://10.162.235.43:4044/jobs?page=1&items=1000&status={0}']
#,'http://10.162.235.44:4044/jobs?page=1&items=1000&status={0}','http://10.162.233.202:4044/jobs?page=1&items=1000&status={0}','http://10.162.235.74:4044/jobs?page=1&items=1000&status={0}']
#,'http://10.162.233.193:4044/jobs?status={0}&items=1000','http://10.162.233.194:4044/jobs?status={0}&items=1000'

CLUSTER_URL=[]
#得到某节点active/failed任务的jobElement,这里并没有合并，怕又那种需求，像得到stop这样的
def getJobElement(num,State='failed'):
    #print(os.getcwd())
    #writer = open("job.txt", "w")
    a=0
    list=[]
    url=CLUSTER_URL[int(num)].format(State)
    #session = requests.session()
    #session.keep_alive = False
    reponse = requests.get(url, headers=headers)
    html_str = reponse.content.decode('gbk')
    reponse.close()
    html = etree.HTML(html_str)
    #result = etree.tostring(html, encoding='utf-8')  # 解析对象输出代码
    #print(result)
    jobs=html.xpath("//tbody//tr[@id]")
    return jobs

#得到你想要的节点的的jobElementList，nums为你想要的server，本质上这个方法和getJobElement是一样的，不过这个过滤出了failed界面的异常
def getFailedOrActiveJobElementList(nums:list,State='failed'):
    jobElementList = []
    for num in nums:
        if State == "stopped":
            jobs = getJobElement(num,"failed")
        else:
            jobs = getJobElement(num,State)
        for job in jobs:
            jobID = job.xpath("./@id")
            isFailed = ""
            isStopped = ""
            #result = etree.tostring(job, encoding='utf-8')  # 解析对象输出代码
            if (CLUSTER_URL[0] == ClusterLocation.LANGFANG):
                isFailed = job.xpath("./td[7]//span/text()")
                isStopped = job.xpath("./td[6]//span/text()")
            elif(CLUSTER_URL[0] == ClusterLocation.HUHE):
                isFailed = job.xpath("./td[9]//span/text()")
                isStopped = job.xpath("./td[8]//span/text()")
            else:
                #这里应该抛出个异常以后再说吧。。
                pass
            #print(isFailed)
            if(State=='failed'):
                if('failed' not in isFailed[0]):
                    continue
                else:
                    #pirnt("")
                    jobElementList.append(job)
            elif State=='stopped':
                if('failed' not in isStopped[0]):
                    continue
            else:
                jobElementList.append(job)
    return jobElementList
"""
    得到server的名字，例如server1
    参数url例如：http://10.177.18.216:4044/jobs?page=1&items=1000&status
"""
def  getServerName(url):
    serverIP = re.search("http://(.*):4044/jobs",url).group(1)
    return serverNumDist[serverIP]

#得到jobElements的sql
def  getSQL(nums:list,State='failed'):
    jobElements = getFailedOrActiveJobElementList(nums,State)
    sqlFile = open("sql.txt", "w")
    for jobElement in jobElements:
        #print(jobElement)
        if(CLUSTER_URL[0] == ClusterLocation.HUHE):
            sqlElementText = jobElement.xpath("./td[4]/div/div/em/text()")[0]
        elif(CLUSTER_URL[0] == ClusterLocation.LANGFANG):
            sqlElementText = jobElement.xpath("./td[2]/div/div/em/text()")[0]
        #print(sqlElementText)
        #print(sqlElementText)
        #注意：这里的group没数据会报异常，所以
        sqlText = re.search("into\s(.*?)_str",sqlElementText).group(1)
        if('context_info' in sqlText):
            sqlFile.write("入库任务:")
            sqlText = re.search("from\s(.*?)_str",sqlElementText).group(1)
            sqlFile.write("use app manager_"+sqlText+"_insert_job_app;\n")
            sqlFile.write("start streamjob manager_"+sqlText+"_insert_job;\n")
        else:
            sqlFile.write("use app "+sqlText+"_app;\n")
            sqlFile.write("start streamjob "+sqlText+"_str_job;\n")
        #print()
        #print(sum)
    sqlFile.close()
    print("sql语句已写入sql.txt")

#单个job在哪个server  
#默认是空，空就是failed and active，acitve就是活动的，failed就是失败的，stop就是正常停止的，还有一个completed状态先放在这边
#加入颜色标记
def  getJobFromWhichServer(job,states=['active','failed']):
    print("开始查找:")
    #获取server个数
    serverIndexsList = list(range(1,len(CLUSTER_URL)))
    #
    finishServerSumInt = 1

    failedServerNumList=[0]
    #遍历所有server
    for serverIndex in serverIndexsList:
        #遍历每个server的各种状态
        for state in states:
            #这个判断其实不需要了，因为我已经在getFailedOrActiveJobElementList方法中作出了更改
            if(state == "failed"):
                failedServerNumList[0] = serverIndex
                #获取相应server相应状态的job的xpath的节点
                jobElements = getFailedOrActiveJobElementList(failedServerNumList,state)
            else:
                if(state == "stopped"):
                    jobElements = getJobElement(serverIndex,"failed")
                else:
                    jobElements = getJobElement(serverIndex,state)
            #将对应serverNum的url提取出来
            url = CLUSTER_URL[serverIndex];
            #得到server的名字，例如：server1，这个数字和index是不同的index是list中的index，而这个数字是集群的，因为这个数字是跳跃的所以和index不同
            serverName = getServerName(url)
            
            #print("server"+str(serverNumReal)+":"+State+"开始查找:")
            for jobElement in jobElements:
                #print(jobElement)
                #提取job的sql
                if(CLUSTER_URL[0] == ClusterLocation.HUHE):
                    sqlElementText = jobElement.xpath("./td[4]/div/div/em/text()")[0]
                elif(CLUSTER_URL[0] == ClusterLocation.LANGFANG):
                    sqlElementText = jobElement.xpath("./td[2]/div/div/em/text()")[0]
#获取任务的提交时间，运行时间戳，提交时间戳
                if(CLUSTER_URL[0] == ClusterLocation.HUHE):
                    runTimestamp = jobElement.xpath("./td[7]/@sorttable_customkey")[0]
                    startTime = jobElement.xpath("./td[6]/text()")[0]
                    startTimestamp = jobElement.xpath("./td[6]/@sorttable_customkey")[0]
                elif(CLUSTER_URL[0] == ClusterLocation.LANGFANG):
                    runTimestamp = jobElement.xpath("./td[5]/@sorttable_customkey")[0]
                    startTime = jobElement.xpath("./td[4]/text()")[0]
                    startTimestamp = jobElement.xpath("./td[4]/@sorttable_customkey")[0]
#获取任务运行至 某个时间点
                runToTime = time.strftime("%Y/%m/%d %H:%M:%S",time.localtime(float((int(str(startTimestamp)) + int(str(runTimestamp))) / 1000)))
                #去掉提交时间中的换行符和空格
                startTime = str(startTime).replace("\r","").replace("\n","").strip()
                try:
                    sqlText = re.search("into\s(.*?)_str",sqlElementText).group(1)
                except AttributeError:
                    continue
                if('context_info' in sqlText):
                    sqlText = re.search("from\scontextdb\.(.*?)_str",sqlElementText).group(1)
                    if ("manager_"+sqlText+"_insert_job" == job):
                        if(state == "stopped"):
                            print("server"+str(serverName)+":"+state+":启动时间："+startTime+"运行至："+runToTime+":\n    "+url.format('failed'))
                        else:
                            print("server"+str(serverName)+":"+state+":启动时间："+startTime+"运行至："+runToTime+":\n    "+url.format(state))
                else:
                    if(sqlText+"_str_job" == job or sqlText+"_str_job" == "contextdb."+job):
                        if(state == "stopped"):
                            print("server"+str(serverName)+":"+state+":启动时间："+startTime+"运行至："+runToTime+":\n    "+url.format('failed'))
                        else:
                            print("server"+str(serverName)+":"+state+":启动时间："+startTime+"运行至："+runToTime+":\n    "+url.format(state))
            #print("  "+state+"查找完成")
            #print("   server"+str(serverNumReal)+":"+state+"查找完成")
        print(str(round((finishServerSumInt/len(serverIndexsList)*100)))+"%",end="\r")
        #print(">",end='')
        finishServerSumInt = finishServerSumInt + 1
    print("server查找完成")
            
#检查是否有新任务失败
#着急：1、必须加入事务2、日志
def is_fail(CLUSTER_URL):
    path = sys.path[0]
    a=range(0,len(CLUSTER_URL)-1)
    if(CLUSTER_URL[0]==ClusterLocation.LANGFANG):
        filePath = path + '/LANGFANGFailTaskNum.txt'
        #file = "a.txt"
    elif(CLUSTER_URL[0]==ClusterLocation.HUHE):
        filePath = path + '/HUHEFailTaskNum.txt'
        #file = "b.txt"
    else:
        print("205行有问题")
    wr=open(filePath,"r")
    flag=[]
    for line in wr.readlines():
        flag.append(int(line.strip("\n")))
    wr.close()
    wr=open(filePath,"w")
    for x in a:
        jobElementsList=getFailedOrActiveJobElementList([x+1])
        #print(list)
        #print(str(len(list)))
        failedTaskNum = len(jobElementsList) - flag[x]
        url = CLUSTER_URL[x+1];
        #得到server的名字，例如：server1，这个数字和index是不同的index是list中的index，而这个数字是集群的，因为这个数字是跳跃的所以和index不同
        serverName = getServerName(url)
        if failedTaskNum > 0:
            print("-----------------------------------------------------")
            print("server"+str(serverName)+"共失败任务数:"+str(len(jobElementsList)))
            print("-----------有"+str(failedTaskNum)+"个新增失败")
            print("关于失败任务的详细信息：")
            for jobElement in jobElementsList:
                #获取任务名字
                if(CLUSTER_URL[0] == ClusterLocation.HUHE):
                    sqlElementText = jobElement.xpath("./td[4]/div/div/em/text()")[0]
                elif(CLUSTER_URL[0] == ClusterLocation.LANGFANG):
                    sqlElementText = jobElement.xpath("./td[2]/div/div/em/text()")[0]
                try:
                    sqlText = re.search("into\s(.*?)_str",sqlElementText).group(1)
                    jobName=""
                    if('context_info' in sqlText):
                        sqlText = re.search("from\s(.*?)_str",sqlElementText).group(1)
                        jobName = "manager_"+sqlText+"_insert_job"
                    else:
                         jobName = sqlText + "_str_job"
                except AttributeError:
                    print("不是正规的流任务"+sqlElementText)
                    jobName=""
                    continue
                

                #获取任务的提交时间，运行时间戳，提交时间戳
                if(CLUSTER_URL[0] == ClusterLocation.HUHE):
                    runTimestamp = jobElement.xpath("./td[7]/@sorttable_customkey")[0]
                    startTime = jobElement.xpath("./td[6]/text()")[0]
                    startTimestamp = jobElement.xpath("./td[6]/@sorttable_customkey")[0]
                elif(CLUSTER_URL[0] == ClusterLocation.LANGFANG):
                    runTimestamp = jobElement.xpath("./td[5]/@sorttable_customkey")[0]
                    startTime = jobElement.xpath("./td[4]/text()")[0]
                    startTimestamp = jobElement.xpath("./td[4]/@sorttable_customkey")[0]
                #获取任务运行至 某个时间点
                runToTime = time.strftime("%Y/%m/%d %H:%M:%S",time.localtime(float((int(str(startTimestamp)) + int(str(runTimestamp))) / 1000)))
                #去掉提交时间中的换行符和空格
                startTime = str(startTime).replace("\r","").replace("\n","").strip()
                #输出
                print(jobName+":启动时间："+startTime+"运行至："+runToTime+":\n    "+url.format('failed'))

            print("-----------------------------------------\n")
        else:
            print("server"+str(serverName)+"正常")
        wr.write(str(len(jobElementsList))+"\n")
    wr.close()

#查找重复的任务
def get_rept(State='active'):
    list=[]
    for url in CLUSTER_URL:
        url=url.format(State)
        print(url)
        s = requests.session()
        s.keep_alive = False
        reponse = requests.get(url, headers=headers)
        html_str = reponse.content.decode('gbk')
        html = etree.HTML(html_str)
        sqls = html.xpath("//tbody//tr[@id]//div[@title]")
        #print(sqls)
        #print
        #writer.write( "--------server"+str(a)+"------------------------------\n")
        for sql in sqls:
             patten=re.search("(?<=into\s).*?(?=\sselect)",sql[0].text)
             str1=patten.group()
             list.append(str1)
    set1=set(list)
    for x in set1:
        sum=list.count(x)
        if sum > 1:
           print(x)
#根据提交时间或结束时间去筛选任务


#1、得到active或者fail的sql //getSQL(num='',list=[]),num就是server几，list为空（全部的active）或3、返回的值
#2、得到失败任务的信息 //getmess_fail(list,f_time='',State='failed')list为3、返回的值，
#3、得到失败任务的jobid //getID_fail(num,State='failed')参数为server几例如：1
#4、单个job在哪个server //参数为(app,State='active/failed')
#5、检查是否有新任务失败 //不需要参数，生产：is_fail1()  呼和：is_fail2()
#6、查找重复的任务 //不使用
def run():
    #list = getFailedOrActiveJobElementList([2],"active")
    #getSQL([2],"active")
    #getJobElement(2)
    #list = getID_fail(2)
    global CLUSTER_URL
    clusterNum=input("选择集群:\n 5.廊坊\n 6.呼和\n")
    if(clusterNum == '5'):
        CLUSTER_URL=CLUSTER_URL_LANGFANG
    elif(clusterNum == '6'):
        CLUSTER_URL=CLUSTER_URL_HUHE
    else:
        print("没有这个集群")
        sys.exit(1)
    actionNum=input("选择功能:\n  1.is_fail()\n  2.getJobFromWhichServer()\n  3.getSQL()\n  4.querySameJob\n")
    if(actionNum == '1'):
        is_fail(CLUSTER_URL)
    elif actionNum == '2':
        #print(CLUSTER_URL)
        #这里加个格式的检验，防止用户输错格式
        streamjob = input("输入streamjob名字:\n")

        #使用,分割吧
        streamjobStatus = input("输入要查找的状态:默认为1&2 1.active  2.failed  3.stopped 4.complted\n")
        statusList = []
        if streamjobStatus!="":
            if "1" in streamjobStatus:
                statusList.append('active')
            if "2" in streamjobStatus:
                statusList.append('failed')        
            if "3" in streamjobStatus:
                statusList.append('stopped')
            if "4" in streamjobStatus:
                statusList.append('complted')
        else:
            statusList.append('active')
            statusList.append('failed')
        getJobFromWhichServer(streamjob,statusList)
    elif actionNum == '3':
        #serverNum = input("请输入要获取的serverNumber（使用","分割，默认为全部的server）:\n")
        #streamjobStatus = input("输入要获取的状态:默认为2 1.active  2.failed  3.stopped 4.complted\n")
        getSQL([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],'failed')
    elif actionNum == '4':
        pass
    #这里加个日期时间格式的检验防止用户输错
    elif actionNum == '5':
        pass
####getJobFromWhichServer
    #getJobFromWhichServer("loc_trade_str_job")

####isfail
    #clusterNum=input("is_fail():\n 5.廊坊\n 6.呼和\n")
    #if(clusterNum == '5'):
    #    CLUSTER_URL=CLUSTER_URL_LANGFANG
    #elif(clusterNum == '6'):
    #    CLUSTER_URL=CLUSTER_URL_HUHE
    #else:
    #    print("没有这个集群")
    #print(type(CLUSTER_URL[0]))
    #is_fail(CLUSTER_URL)
    


    #getSQL([1],'active')


if __name__ == '__main__':
    run()