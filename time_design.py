import datetime


# 创建时间戳
def time_stamp():
    start_date = datetime.datetime(1970, 2, 1)
    now = datetime.datetime.now()  # 即时日期
    if now.month < 12:  # 如果即时的月份小于12
        end_date = datetime.datetime(now.year, now.month + 1, 1) - datetime.timedelta(days=1)
        # 月份+1
    else:  # 如果即时月份为12月
        end_date = datetime.datetime(now.year + 1, 1, 1) - datetime.timedelta(days=1)
        # 年份+1
    return start_date, end_date


# 修改时间戳格式为所要
def time_justify():
    now = datetime.datetime.now()  # 开始时间戳
    end = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)  # 结束时间戳
    if (end.month == 10) or (end.month == 11) or (end.month == 12):  # 两位数月份格式
        endDate = str(end.year) + '-' + str(end.month) + '-' + str(end.day)
    else:  # 一位数月份格式
        endDate = str(end.year) + '-0' + str(end.month) + '-' + str(end.day)
    return endDate