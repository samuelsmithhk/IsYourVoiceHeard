import csv


class MemberStoreDAO:

    member_store = None

    @staticmethod
    def get_member_store():
        return MemberStoreDAO.member_store

    @staticmethod
    def init_member_store(member_file_path):
        MemberStoreDAO.member_store = MemberStore(member_file_path)


class MemberStore:

    def __init__(self, house_file_path):
        self.house_map = {}
        self.senate_map = {}

        with open(house_file_path, 'r') as house_file:

            house_csv = csv.DictReader(house_file)
            i = 0

            for row in house_csv:
                state = row['state'].lower()
                name = row['congressman_full_name']
                district = row['district']

                member = Member(i, name, None, district, None)

                if state in self.house_map:
                    self.house_map[state].append(member)
                else:
                    self.house_map[state] = [member]

                i += 1

    def is_state(self, state):
        return state.lower() in self.house_map

    def get_congressmen_in_state_json(self, state):
        members = self.house_map[state.lower()]
        members_json = []

        for member in members:
            members_json.append({
                'name': member.name,
                'district': member.district
            })

        return members_json


class Member:

    def __init__(self, id_number, name, party, district, photo_url):
        self.id_number = id_number
        self.name = name
        self.party = party
        self.district = district
        self.photo_url = photo_url
