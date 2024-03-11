import datetime
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

# 创建一个 logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # 设置 logger 级别为 DEBUG

# 创建一个 handler，用于写入日志文件
# midnight: 表示日志文件在每天半夜时分滚动
# interval: 间隔时间单位的个数，指等待多少个 when 的时间后 Logger 会自动重建新闻继续进行日志记录
# backupCount: 表示日志文件的保留个数，假如为7，则会保留最近的7个日志文件
save_handler = TimedRotatingFileHandler("logs/log", when="midnight", interval=1, backupCount=14)
save_handler.suffix = "%Y-%m-%d"  # 设置日志文件名的时间戳格式

# 创建一个 handler，用于输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # 设置 handler 级别为 INFO

# 创建一个 formatter，用于设置日志格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 设置 handler 的格式
save_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 为 logger 添加 handler
logger.addHandler(save_handler)
logger.addHandler(console_handler)

# 记录日志
logger.info('This is a log info')
logger.debug('Debugging')
logger.warning('Warning exists')
logger.error('Error!')

import json
import uuid

from apscheduler.schedulers.blocking import BlockingScheduler
from flask import Flask, request, send_file, send_from_directory, make_response
from flask_apscheduler import APScheduler
from flask_cors import CORS
from gevent import pywsgi

from clickhouse import ClickHouse

import os
import time

from count import run
from SchedulerConfig import Config

database = ClickHouse('10.26.22.117', 9001, 'default', 'galaxy2019', 'new')

app = Flask(__name__)
CORS(app, resources=r'/*')


@app.post('/feedback')
def upload_feedback():
    required_fields = ['agent_type', 'feedback_type', 'feedback']
    for field in required_fields:
        if field not in request.form:
            # 返回错误信息，json格式，code为402，message为错误信息
            return json.dumps({'code': 402, 'message': f"缺少必要字段{field}"})
    try:
        agent_type = int(request.form['agent_type'])
        feedback_type = int(request.form['feedback_type'])
        feedback = str(request.form['feedback']).replace("'", "''")
        feedback = feedback.replace('"', '""')
        feedback = feedback.replace(';', '；')
    except Exception as e:
        logging.error(f"参数类型错误: {e}")
        # 返回错误信息，json格式，code为402，message为错误信息
        return json.dumps({'code': 402, 'message': f"参数类型错误"})

    database.connect()
    if 'file' not in request.form or request.form['file'] == 'undefined':
        try:
            logging.info(f"上传无文件反馈: {agent_type} {feedback_type} {feedback}")
            database.execute(
                "INSERT INTO FEEDBACK (ID, AGENT_TYPE, FEEDBACK_TYPE, FEEDBACK, FEEDBACK_TIME, IS_COMPLETED) "
                f"VALUES (generateUUIDv4(), {agent_type}, {feedback_type}, '{feedback}', now(), 0)")
        except Exception as e:
            logging.error(f"存储无文件反馈失败: {e}")
            # 返回错误信息，json格式，code为400，message为错误信息
            return json.dumps({'code': 422, 'message': str(e)})
        finally:
            database.close()
        return json.dumps({'code': 200, 'message': 'success'})
    else:
        return json.dumps({'code': 422, 'message': '关闭文件上传功能'})


@app.get('/feedback')
def get_feedback():
    # 获取输入条件，数量（必填） 时间范围（可选）
    required_fields = ['page', 'size']
    for field in required_fields:
        if field not in request.args:
            # 返回错误信息，json格式，code为400，message为错误信息
            return json.dumps({'code': 400, 'message': f"缺少必要字段{field}"})
    try:
        page = int(request.args['page'])
        size = int(request.args['size'])
        start_time = request.args.get('start_time', '1970-01-01 00:00:00')
        end_time = request.args.get('end_time', '2100-01-01 00:00:00')
    except Exception as e:
        logging.error(f"参数类型错误: {e}")
        # 返回错误信息，json格式，code为402，message为错误信息
        return json.dumps({'code': 402, 'message': f"参数类型错误"})
    logging.info(f"get_feedback: {page} {size} {start_time} {end_time}")
    database.connect()
    try:
        total = database.execute(
            f"SELECT COUNT(*) FROM FEEDBACK WHERE FEEDBACK_TIME >= '{start_time}' AND FEEDBACK_TIME <= '{end_time}';")
        total = int(total[0][0])
        if total == 0:
            logging.warning(f"查询反馈失败: 时间段内无数据")
            return json.dumps({'code': 200, 'message': 'success', 'data': [], 'total': total})
        result = database.execute(
            f"SELECT ID, AGENT_TYPE, FEEDBACK_TYPE, FEEDBACK, FILE_PATH, FEEDBACK_TIME, IS_COMPLETED "
            f"FROM FEEDBACK WHERE FEEDBACK_TIME >= '{start_time}' AND FEEDBACK_TIME <= '{end_time}' "
            f"ORDER BY FEEDBACK_TIME DESC LIMIT {(page - 1) * size}, {size};")
        total_page = total // size + 1
        if len(result) == 0:
            logging.warning(f"查询反馈失败: 无数据")
            return json.dumps({'code': 200, 'message': 'success', 'data': [], 'total_page': total_page, 'total': total})
        data = []
        for i in range(len(result)):
            item = {}
            result[i] = list(result[i])
            item['id'] = str(result[i][0])
            item['agent_type'] = result[i][1]
            item['feedback_type'] = result[i][2]
            item['feedback'] = result[i][3]
            item['feedback_time'] = str(result[i][5])
            data.append(item)
            logging.debug(item)
    except Exception as e:
        logging.error(f"获取反馈失败: {e}")
        # 返回错误信息，json格式，code为400，message为错误信息
        return json.dumps({'code': 422, 'message': str(e)})
    finally:
        database.close()
    logging.info(f"获取反馈成功")
    return json.dumps({'code': 200, 'message': 'success', 'data': data, 'total_page': total_page, 'total': total})


@app.get('/feedback/download')
def download_feedback():
    return json.dumps({'code': 422, 'message': '关闭文件下载功能'})
    # 获取文件路径（必填）
    required_fields = ['file_path']
    for field in required_fields:
        if field not in request.args:
            # 返回错误信息，json格式，code为400，message为错误信息
            return json.dumps({'code': 400, 'message': f"缺少必要字段{field}"})
    file_path = str(request.args['file_path'])
    logging.info(f"下载文件:{file_path}")
    dir = os.path.dirname(file_path)
    # print(dir)
    file_name = file_path.split('/')[-1].split('___')[-1]
    # print(file_name)
    # 获取程序所在位置
    try:
        return make_response(send_file(file_path, download_name=file_name, as_attachment=True))
    except Exception as e:
        logging.error(f"下载文件失败: {e}")
        # 返回错误信息，json格式，code为400，message为错误信息
        return json.dumps({'code': 422, 'message': str(e)})


def addition_data(data, in_list):
    for item in data:
        sni, count = item.split('：')
        if sni not in in_list:
            in_list[sni] = 0
        in_list[sni] += float(count)
    return in_list


def reform_data(data):
    data = sorted(data.items(), key=lambda x: x[1], reverse=True)
    for i in range(len(data)):
        if data[i][1] > 0:
            data[i] = {'name': data[i][0], 'count': '{:.2f}'.format(data[i][1])}
    return data


def parse_data(data):
    ips = {}
    ip_byte = {}
    snis = {}
    sni_byte = {}
    for i in range(len(data)):
        snis = addition_data(data[i][1], snis)
        ips = addition_data(data[i][2], ips)
        ip_byte = addition_data(data[i][3], ip_byte)
        sni_byte = addition_data(data[i][4], sni_byte)
    ips = reform_data(ips)
    snis = reform_data(snis)
    ip_byte = reform_data(ip_byte)
    sni_byte = reform_data(sni_byte)
    return [data[-1][0], ips, snis, ip_byte, sni_byte]


@app.get('/data_stats')
def get_data_stats():
    logging.info(f"获取数据统计")
    try:
        agent_type = request.args['type']
    except Exception as e:
        logging.error(f"参数错误: {e}")
        # 返回错误信息，json格式，code为402，message为错误信息
        return json.dumps({'code': 402, 'message': f"参数错误, 参数type为必填项"})
    try:
        # 获取今天的日期时间
        now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        last = now - datetime.timedelta(hours=1)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        last_str = last.strftime("%Y-%m-%d %H:%M:%S")
        database.connect()
        if agent_type == '2':
            data = database.execute(
                f"SELECT COUNT_TIME, SSL_SNI, CLIENT_IP, BYTE_CLIENT, BYTE_SNI FROM new.SSL_LOG_RESULT WHERE TYPE = 3 "
                f"AND COUNT_TIME >= '{last_str}'")
            if len(data) == 0:
                logging.warning(f"获取数据统计失败: 无数据")
                database.close()
                return json.dumps({'code': 200, 'message': 'success', 'data': {}})
            result = parse_data(data)
        else:
            data = database.execute(
                f"SELECT COUNT_TIME, SSL_SNI, CLIENT_IP, BYTE_CLIENT, BYTE_SNI FROM new.SSL_LOG_RESULT "
                f"WHERE TYPE = {agent_type} "
                f"AND COUNT_TIME >= '{last_str}'")
            if len(data) == 0:
                logging.warning(f"获取数据统计失败: 无数据")
                database.close()
                return json.dumps({'code': 200, 'message': 'success', 'data': {}})
            result = parse_data(data)
        result = {'count_time': str(result[0]), 'client_ip': result[1][:10], 'ssl_sni': result[2][:100],
                      'byte_client_ip': result[3][:10], 'byte_ssl_sni': result[4][:100]}
    except Exception as e:
        logging.error(f"获取数据统计失败: {e}")
        # 返回错误信息，json格式，code为400，message为错误信息
        return json.dumps({'code': 422, 'message': str(e)})
    finally:
        database.close()
    return json.dumps({'code': 200, 'message': 'success', 'data': result})


@app.get('/data_stats/count')
def get_data_stats_count():
    logging.info(f"获取数据统计")
    database.connect()
    result = {}
    start_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    start_time_str = start_time.strftime("%Y-%m-%d")
    now = start_time
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"获取数据统计: {start_time_str}-{now_str}")

    for i in range(3):
        try:
            if i == 0:
                count = database.execute(
                    f"SELECT count(distinct CLIENT_IP), sum(S2C_BYTE_NUM + C2S_BYTE_NUM) / 1024 "
                    f"FROM new.SSL_COLLECT_LOG "
                    f"WHERE RECV_TIME >= '{start_time_str} 00:00:00' AND SSL_SNI != '' and CLIENT_IP like '192.168.%' "
                    f"AND DEVICE_IP='10.28.8.76' AND SSL_SNI not like '%gstatic%' AND SSL_SNI not like '%googleapis%' "
                    f"AND SSL_SNI not like '%microsoft%' AND SSL_SNI not like '%baidu%' "
                    f"AND SSL_SNI != 'redirector.gvt1.com';")
            elif i == 1:
                count = database.execute(
                    f"SELECT count(distinct CLIENT_IP), sum(S2C_BYTE_NUM + C2S_BYTE_NUM) / 1024 "
                    f"FROM shucun87.TCP_UDP_COLLECT_LOG "
                    f"WHERE RECV_TIME >= '{start_time_str} 00:00:00' AND CLIENT_IP like '192.168.%' "
                    f"AND L7_PROTOCOL=='HTTP;SSL';")
            else:
                count = database.execute(
                    f"SELECT count(distinct CLIENT_IP), sum(S2C_BYTE_NUM + C2S_BYTE_NUM) / 1024 "
                    f"FROM shucun87.SSL_COLLECT_LOG "
                    f"WHERE RECV_TIME >= '{start_time_str} 00:00:00' AND CLIENT_IP like '192.168.%' "
                    f"AND SSL_SNI = 'fireproxy.xyz';")
        except Exception as e:
            logging.error(f"获取数据统计失败: {e}")
            database.close()
            # 返回错误信息，json格式，code为400，message为错误信息
            return json.dumps({'code': 422, 'message': str(e)})
        if len(count) == 0:
            logging.warning(f"获取数据统计失败: 无数据")
            result[i] = {'count_time': f'{now_str}', 'client_count': 0, 'byte_size': 0, 'type': i}
            continue
        result[i] = {'count_time': f'{now_str}', 'client_num': count[0][0], 'byte_size': int(count[0][1])}
    database.close()
    return json.dumps({'code': 200, 'message': 'success', 'data': result})


@app.get('/data_stats/history')
def get_data_stats_history():
    logging.info(f"获取历史数据统计")
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    cur_str = now.strftime("%Y-%m-%d %H:%M:%S")
    now = now - datetime.timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    today = now.strftime("%Y-%m-%d")
    query = [{'table': 'new.SSL_COLLECT_LOG', 'where': " DEVICE_IP='10.28.8.76'  AND SSL_SNI != '' AND SSL_SNI not like '%microsoft%' AND SSL_SNI != 'redirector.gvt1.com' AND SSL_SNI not like '%googleapis%' AND SSL_SNI not like '%baidu%' AND SSL_SNI not like '%gstatic%' ",
              'col': 'CLIENT_IP'},
             {'table': 'shucun87.TCP_UDP_COLLECT_LOG', 'where': "L7_PROTOCOL=='HTTP;SSL'",
              'col': 'CLIENT_IP'},
             {'table': 'shucun87.SSL_COLLECT_LOG', 'where': "SSL_SNI = 'fireproxy.xyz'", 'col': 'CLIENT_IP'}]
    try:
        agent_type = request.args['type']
    except Exception as e:
        logging.error(f"参数类型错误: {e}")
        # 返回错误信息，json格式，code为402，message为错误信息
        return json.dumps({'code': 402, 'message': f"参数错误, 参数type为必填项"})
    start_time = request.args.get('start_time', today)
    end_time = request.args.get('end_time', now_str)
    logging.info(f"查询时间:{start_time}-{end_time}")
    result = []
    try:
        database.connect()
        result = database.execute(
            f"SELECT *  FROM new.SSL_COUNT_RESULT WHERE TYPE = {agent_type} AND "
            f"COUNT_TIME >= '{start_time}' "
            f"ORDER BY COUNT_TIME;")
        for i in range(len(result)):
            result[i] = list(result[i])
            result[i][0] = str(result[i][0])
            result[i][1] = int(result[i][1])
            result[i][2] = int(result[i][2])
            result[i][3] = int(result[i][3])
            result[i][4] = int(result[i][4])
            result[i] = {'count_time': result[i][0], 'client_count': result[i][1],
                         'byte_size': result[i][2] + result[i][4], 'type': result[i][3]}
        try:

            data = database.execute(f"SELECT COUNT(DISTINCT CLIENT_IP), sum(C2S_BYTE_NUM) / 1024, sum(S2C_BYTE_NUM) / 1024 "
                                    f"FROM {query[int(agent_type)]['table']} "
                                    f"WHERE RECV_TIME >= '{now_str}' AND CLIENT_IP like '192.168.%' "
                                    f"AND {query[int(agent_type)]['where']};")
            result.append({'count_time': f'{cur_str}', 'client_count': int(data[0][0]),
                           'byte_size': int(data[0][1] + data[0][2]), 'type': agent_type})
        except Exception as e:
            logging.error(f"SELECT COUNT(DISTINCT CLIENT_IP), sum(C2S_BYTE_NUM) / 1024, sum(S2C_BYTE_NUM) / 1024 "
                                    f"FROM {query[int(agent_type)]['table']} "
                                    f"WHERE RECV_TIME >= '{now_str}' "
                                    f"AND {query[int(agent_type)]['where']};")
            logging.error(f"统计{query[0]['table']}失败: {e}")
            data = [(0, 0, 0)]
    except Exception as e:
        logging.error(f"获取历史数据统计失败: {e}")
        # 返回错误信息，json格式，code为400，message为错误信息
        return json.dumps({'code': 422, 'message': str(e)})
    finally:
        database.close()
    return json.dumps({'code': 200, 'message': 'success', 'data': result})


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler = APScheduler()
    scheduler.init_app(app)  # 将调度器对象与Flask应用程序实例(app)相关联
    scheduler.start()
    server = pywsgi.WSGIServer(('0.0.0.0', 5000), app)
    server.serve_forever()
    # app.run(host='0.0.0.0', port=5000, debug=True)
    # scheduler = BlockingScheduler()
    # scheduler.add_job(run, 'cron', [database], minute='0/5')
    # scheduler.start()
