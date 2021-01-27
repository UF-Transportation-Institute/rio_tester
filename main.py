import socket
import csv
import argparse
from datetime import datetime, timezone
import time

def send_to_rio(ip, port):
    _bsm_publisher = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _bsm_publisher.setblocking(0)

    today = datetime.utcnow()

    with open('messages.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|',quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            data = ''.join(row)
            print('input msg',data)
            time_string = data.split('^^^^')[0]
            
            # Needs to add a full day/month/year in order to do operations with timestamp.
            # This timestamp needs to be transferred to the proper UTC timezone.
            new_combined_time = datetime.combine(today, datetime.strptime(time_string, '%H:%M:%S.%f').time())
            
            try:
                delta_time = new_combined_time - old_combined_time
                delta_time_secs = delta_time.total_seconds()

                # This check exists because sometimes the messages on the csv file are not 
                # in chronological order.
                if  delta_time_secs >0:
                    print('delta time secs',delta_time_secs)
                    time.sleep(delta_time_secs)
                else:
                    print('negative delta time')
                
                # prev_time is the timestamp that is sent to RIO.
                prev_time = prev_time + delta_time
                old_combined_time = new_combined_time

            except:
                print('no previous time')
                prev_time = today
                old_combined_time = new_combined_time

            final_msg = '{}:{}:{}.{}^^^^'.format(prev_time.hour,prev_time.minute,
                                                 prev_time.second,prev_time.microsecond)

            final_msg = final_msg+data.split('^^^^')[1]

            print('output msg',final_msg,'\n')
            _bsm_publisher.sendto(final_msg.encode(), (ip, port))
            

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    my_parser = argparse.ArgumentParser(description='RIO Server Info')
    my_parser.add_argument('IP',
                           metavar='ip',
                           type=str,
                           help='ip address')

    my_parser.add_argument('Port',
                           metavar='port',
                           type=int,
                           help='port')
    args = my_parser.parse_args()

    send_to_rio(args.IP, args.Port)
