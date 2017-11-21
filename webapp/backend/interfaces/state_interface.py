import json

from tornado.web import RequestHandler
from model.member import MemberStoreDAO


class StateInterface(RequestHandler):

    def get(self, state):
        member_store = MemberStoreDAO.get_member_store()

        if member_store.is_state(state):
            congressmen = member_store.get_congressmen_in_state_json(state)

            response = {
                "status": "success",
                "congressmen": congressmen
            }
            self.set_status(200)
        else:
            response = {
                "status": "error",
                "message": "Invalid state"
            }
            self.set_status(400)

        response_json = json.dumps(response)
        print(response_json)
        self.write(response_json)
        self.flush()

    def data_received(self, chunk):
        pass
