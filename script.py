from optparse import OptionParser
import redis
import ast


def main():
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="choose log file name", metavar="FILE")
    parser.add_option("-U", "--upload",
                      action="store_true", dest="is_upload",
                      help="parse log file and upload data to database")
    parser.add_option("-T", "--time",
                      action="store_true", dest="time_range",
                      help="chose time range to view stats in time range")
    parser.add_option("-C", "--chanel", dest="chanel",
                      help="chose chanel for filter")
    (options, args) = parser.parse_args()
    if options.filename and options.is_upload:
        parse_log(options.filename)
    elif options.chanel and options.time_range:
        filter_chanel_time(options.chanel, *args)
    elif options.time_range:
        get_list_by_time(*args)
    else:
        print('Wrong commands, type --help')


def parse_log(filename):
    redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
    with open(filename, 'r') as filename:
        for string in filename:
            serialize(string.replace('"', '').replace('[', '').replace(']', '').split(), redis_db)


def serialize(log_str, redis_db):
    # if useragent title splitted by backspace
    if len(log_str) == 14:
        log_str[11] += log_str[12]
        log_str[12] = log_str[13]
    log = {'ip': log_str[0],
           'user': log_str[1],
           'auth': log_str[2],
           'datetime': datetime_format(log_str[3]),
           'zone': log_str[4],
           'query_type': log_str[5],
           'chanel': log_str[6],
           'protocol': log_str[7],
           'status': log_str[8],
           'size': log_str[9],
           'referer': log_str[10],
           'useragent': log_str[11],
           'query_time': log_str[12]
           }
    redis_db.zadd('log', log['datetime'], log)


def datetime_format(datetime_str, input=False):
    format_str = datetime_str.replace('/', '').replace(':', '')[:15]
    if not input:
        month_to_digit = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                          'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
        format_str = format_str[5:9] + month_to_digit[format_str[2:5]] + format_str[0:2] + format_str[9:15]
    return int(format_str)


def get_list_by_time(*args):
    if len(args) > 2:
        print('Too many arguments')
        return
    elif args[0] > args[1]:
        print('First range value must be less')
        return
    elif len(args[0]) < 19 or len(args[1]) < 19:
        print('Wrong datetime format, use YYYY/MM/DD:HH:MM:SS, as an example 2017/08/18:09:00:00')
        return
    min_dt = datetime_format(args[0], True)
    max_dt = datetime_format(args[1], True)
    result_from_db = get_from_db_in_range(min_dt, max_dt)
    count_chanel = {}
    bytes_sum = {}
    for res in result_from_db:
        res_parsed = ast.literal_eval(res)
        count_stats(res_parsed, count_chanel, bytes_sum)
    for key, value in count_chanel.items():
        print('Chanel %s - NUM OF QUERYS %s , TOTAL BYTES %s' % (key, value, bytes_sum[key]))


def count_stats(res, count_chanel, bytes_sum):
    if count_chanel.get(res['chanel']):
        count_chanel[res['chanel']] += 1
        bytes_sum[res['chanel']] += int(res['size'])
    else:
        count_chanel[res['chanel']] = 1
        bytes_sum[res['chanel']] = int(res['size'])


def count_stats_chanel(res, count_chanel, bytes_sum, chanel):
    if res['chanel'] == chanel:
        if count_chanel.get(res['chanel']):
            count_chanel[chanel] += 1
            bytes_sum[chanel] += int(res['size'])
        else:
            count_chanel[chanel] = 1
            bytes_sum[chanel] = int(res['size'])
    else:
        if bytes_sum.get('other'):
            bytes_sum['other'] += int(res['size'])
        else:
            bytes_sum['other'] = int(res['size'])


def get_from_db_in_range(min_dt, max_dt):
    redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
    return redis_db.zrangebyscore('log', min_dt, max_dt)


def filter_chanel_time(chanel, *args):
    if len(args) > 2:
        print('Too many arguments')
        return
    elif args[0] > args[1]:
        print('First range value must be less')
        return
    elif len(args[0]) < 19 or len(args[1]) < 19:
        print('Wrong datetime format, use YYYY/MM/DD:HH:MM:SS, as an example 2017/08/18:09:00:00')
        return
    min_dt = datetime_format(args[0], True)
    max_dt = datetime_format(args[1], True)
    result_from_db = get_from_db_in_range(min_dt, max_dt)
    count_chanel = {}
    bytes_sum = {}
    for res in result_from_db:
        res_parsed = ast.literal_eval(res)
        count_stats_chanel(res_parsed, count_chanel, bytes_sum, chanel)
    percent = (float(bytes_sum[chanel])/float(bytes_sum['other'])) * 100
    print('Stats of Chanel %s : TOTAL QUERYS %s , PERCENT OF TOTAL TRAFFIC %s'
            % (chanel, count_chanel[chanel], percent))


main()
