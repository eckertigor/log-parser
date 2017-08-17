from optparse import OptionParser
import redis
import time


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts))
        # return result

    return timed


@timeit
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
    (options, args) = parser.parse_args()
    if options.filename and options.is_upload:
        parse_log(options.filename)
    elif options.time_range:
        print(options.time_range)


def parse_log(filename):
    redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
    pipe = redis_db.pipeline()
    with open(filename, 'r') as filename:
        for i in range(0, 1000000+1):
            serialize(filename.readline().replace('"', '').replace('[', '').replace(']', '').split(), pipe)
    pipe.execute()


def serialize(log_str, pipe):
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
    pipe.zadd('log', log['datetime'], log)


def datetime_format(datetime_str):
    format_str = datetime_str.replace('/', '').replace(':', '')[:15]
    month_to_digit = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                       'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    format_str = format_str[5:9] + month_to_digit[format_str[2:5]] + format_str[0:2] + format_str[9:15]
    return int(format_str)




main()
