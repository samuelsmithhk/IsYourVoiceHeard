import argparse
import requests
import csv

from bs4 import BeautifulSoup


def main_members(output_file):
    print("Parsing house members")

    page_contents = requests.get("http://clerk.house.gov/member_info/olmbr.aspx")
    page = BeautifulSoup(page_contents.text, "html.parser")

    congressmen_cells = page.tbody.find_all('td')
    # data now looks like: [<td>Abraham, Ralph Lee</td>, <td>LA</td>, <td>5th</td>, ...

    congressmen_cells = [row.string for row in congressmen_cells]
    # data now looks like: ['Abraham, Ralph Lee', 'LA', '5th', 'Adams, Alma S.', 'NC', '12th ...

    with open(output_file, 'w') as file_out:
        csv_out = csv.writer(file_out)
        csv_out.writerow(['congressman_full_name', 'state', 'district'])

        buffer = []
        for cell in congressmen_cells:

            buffer.append(cell)

            if len(buffer) == 3:
                if buffer[0] is not None:
                    csv_out.writerow(buffer)
                buffer = []


def main_roll_call(output_file1, output_file2):
    print("Parsing house roll call")

    # First we need to find the latest roll call
    index_contents = requests.get("http://clerk.house.gov/evs/2017/index.asp")
    index = BeautifulSoup(index_contents.text, "html.parser")
    latest_bill = int(index.find_all('td')[0].string)

    base_url = "http://clerk.house.gov/evs/2017/roll"

    with open(output_file1, 'w') as meta_file_out:
        with open(output_file2, 'w') as roll_call_file_out:
            csv_meta_out = csv.writer(meta_file_out)
            csv_rc_out = csv.writer(roll_call_file_out)

            csv_meta_out.writerow(['roll_call_id', 'vote_date', 'vote_description', 'question'])
            csv_rc_out.writerow(['congressman_short_name', 'roll_call_id', 'vote'])

            for i in range(1, latest_bill + 1):
                print("Scraping bill %d of %d" % (i, latest_bill))
                si = "%03d" % i
                page_contents = requests.get(base_url + si + ".xml")
                page = BeautifulSoup(page_contents.text, "html.parser")

                vote_date = page.find('action-date').text
                vote_description = page.find('vote-desc').text
                question = page.find('vote-question').text

                csv_meta_out.writerow([si, vote_date, vote_description, question])

                recorded_votes = page.find_all('recorded-vote')

                for vote in recorded_votes:
                    congressman_short_name = vote.find('legislator').string
                    vote = vote.find('vote').string

                    csv_rc_out.writerow([congressman_short_name, si, vote])

                if i == 10:
                    print(page.prettify())




def main():
    ap = argparse.ArgumentParser("Clerk of the House Data Scraper")
    ap.add_argument('mode', type=str, help="m = members, r = roll call")
    ap.add_argument('--output1', '-o1', type=str, help="Output file 1 (default: ./output1.csv", default='output1.csv')
    ap.add_argument('--output2', '-o2', type=str, help="Output file 2 (default: ./output2.csv", default='output2.csv')
    args = ap.parse_args()

    if args.mode == 'm':
        main_members(args.output1)
    elif args.mode == 'r':
        main_roll_call(args.output1, args.output2)
    else:
        print("Run with m or r. m == members scrape, r == roll call scrape")
        exit(1)


if __name__ == '__main__':
    main()
