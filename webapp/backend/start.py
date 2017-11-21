import argparse

from tornado.ioloop import IOLoop
from interfaces import interface_master
from model.member import MemberStoreDAO


def main():
    ap = argparse.ArgumentParser("Backend of IsYourVoiceHeard")
    ap.add_argument('member_list_house', help='Filepath to the member list csv file of the House')

    args = ap.parse_args()

    MemberStoreDAO.init_member_store(args.member_list_house)

    interface_master.launch_interfaces(28250)

    try:
        IOLoop.current().start()
    except:
        IOLoop.current().stop()


if __name__ == '__main__':
    main()
