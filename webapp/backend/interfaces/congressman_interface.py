from tornado.web import RequestHandler


class CongressmanInterface(RequestHandler):

    def get(self, congressman_id):
        print("TODO")
        # We want to return donors, twitter sentiment for topics, likelihood to vote
        # if proposed by Republican, if proposed by Democrat
        # republican tax
        # democrat tax
        # republican immigration
        # democrat immigration
        # republican gun
        # democrat gun
        # republican other
        # democrat other

    def data_received(self, chunk):
        pass