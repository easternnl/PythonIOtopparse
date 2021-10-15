from datetime import datetime
import glob
import argparse
#import influxdb
from influxdb import InfluxDBClient

# parse the arguments
parser = argparse.ArgumentParser()

parser.add_argument('--filename', help='IOtop logfile to process - wildcard possible', required=True)
parser.add_argument('--date', help='Date to process with the timestamps in yyyy-MM-dd', required=True)
parser.add_argument('--hostname', help='Hostname to send to influx', required=True)
parser.add_argument('--dbhost', default="localhost", help='InfluxDb hostname or ip')
parser.add_argument('--dbport', default="8086", help='InfluxDb port number')
parser.add_argument('--dbname', help='InfluxDb database name', default='iotop')
parser.add_argument('--dbdrop', default=0, help='Drop database if exist to ensure a clean database')
parser.add_argument('--batchsize', default=20000, help='How many inserts into InfluxDb in one chunk')
parser.add_argument('--verbose', default=0, help='Display all parameters used in the script')
parser.add_argument('--debug', default=0, help='Display processing and what is send to influx as line protocol')

args = parser.parse_args()

# Show arguments if verbose
if (args.verbose):    
    print("Filename=%s" %(args.filename))
    print("Batchsize=%d" %(args.batchsize))
    print("Dbhost=%s" %(args.dbhost))
    print("Dbport=%s" %(args.dbport))
    print("Dbname=%s" %(args.dbname))
    print("Dbdrop=%d" %(args.dbdrop))

# connect to influx
client = InfluxDBClient(host=args.dbhost, port=args.dbport)

if (args.dbdrop):
    # delete the database if required
    client.drop_database(args.dbname)

# create a new database and switch to the database
client.create_database(args.dbname)
client.switch_database(args.dbname)

datapoints = []

for filename in glob.glob(args.filename):
    print("File: %s" % (filename))

    with open(filename) as fp:  
        for cnt, line in enumerate(fp):

            # read first column and get the time, combine that with the date to have the actual epoch
            try:
                epoch = datetime.strptime("%sT%s" % (args.date, line[0:8]), '%Y-%m-%dT%H:%M:%S').timestamp()
            except ValueError:
                continue

            # skip the totals for now
            if "Total DISK READ" in line:
                pass
            elif "Actual DISK READ" in line:
                pass
            elif "Current DISK READ" in line:
                pass
            else:
                # process analytics
                #     TIME  TID  PRIO  USER     DISK READ  DISK WRITE  SWAPIN      IO    COMMAND
                # 20:04:37   911 be/4 root        0.00 B/s    0.00 B/s  0.00 % 44.15 % [txg_sync]
                #or
                #    TIME    PID  PRIO  USER     DISK READ  DISK WRITE  SWAPIN      IO    COMMAND
                #19:03:55  533559 be/4 root        0.00 K/s    0.00 K/s  0.00 %  0.47 % [kworker/u256:0-events_power_efficient]
                #
                # replace double spaces with single spaces in three loops
                line = line.replace('  ',' ').replace('  ', ' ').replace('  ',' ').replace('  ',' ').rstrip()   # remove double spaces
                if (args.debug):
                    print(line)

                # then split on space
                items = line.split(' ')
                
                pidtid = items[1]
                prio = items[2]
                user = items[3]
                diskread = float(items[4])
                diskwrite = float(items[6])
                swapin = float(items[8])
                io = float(items [10])
                command = items[12]
                commandarg = ' '.join(items[12:])

                # print("pidtid=%s" % (pidtid))
                # print("prio=%s" % (prio))
                # print("user=%s" % (user))
                # print("diskread=%s" % (diskread))
                # print("diskwrite=%s" % (diskwrite))
                # print("swapin=%s" % (swapin))
                # print("io=%s" % (io))
                # print("command=%s" % (command))

                datapoint = "iotop,hostname=%s" % (args.hostname)
                #datapoint += ",pidtid=%s" % (pidtid)
                datapoint += ",user=%s" % (user)
                datapoint += ",command=%s" % (command)

                datapoint += " "

                #if diskread > 0:
                datapoint += "diskread=%s," % (diskread)
                #if diskwrite > 0:
                datapoint += "diskwrite=%s," % (diskwrite)
                #if swapin > 0:
                datapoint += "swapin=%s," % (swapin)
                #if io > 0:
                datapoint += "io=%s," % (io)

                datapoint = datapoint[:-1]
                
                datapoint += " %d" % ((epoch - 3600) * 1000 * 1000 * 1000)

                if (args.debug):
                    print (datapoint)

                response = client.write_points(datapoint,  protocol ="line")


                pass


        