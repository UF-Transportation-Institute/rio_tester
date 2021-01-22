import socket
import csv
import argparse


def send_to_rio(ip, port):
    _bsm_publisher = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _bsm_publisher.setblocking(0)

    with open('messages.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|',quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            data = ''.join(row)
            print(data)
            _bsm_publisher.sendto(data.encode(), (ip, port))


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
