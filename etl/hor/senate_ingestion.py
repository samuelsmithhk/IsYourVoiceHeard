import requests
import csv
from bs4 import BeautifulSoup

def main():
    page = requests.get("https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_115_1.htm")
    soup = BeautifulSoup(page.text)
    roll_call_table = soup.find_all('tr')[1:]
    summary_file = 'summary.csv'
    vote_file = 'votes.csv'
    with open(summary_file, 'w') as summary_output:
        header = 'roll_call_id,vote_date, title,question'
        summary_output.write(header + '\n')
        with open(vote_file, "w") as vote_output:
            header = 'short_name, roll_call_id,vote'
            vote_output.write(header + '\n')
            for row in roll_call_table:
                #find roll call data
                summary = row.find_all('td')
                roll_call_id = summary[0].get_text().replace('\xa0', ' ').split(' ')[0]
                result = summary[1].get_text()
                title = summary[2].get_text().split(':')[0]
                question = summary[2].get_text().split(':')[1].strip()
                vote_date = summary[4].get_text().replace('\xa0',' ')
                row = ','.join((roll_call_id, vote_date, title, question))
                summary_output.write(row + '\n')

                #find roll call page and find votes records    
                vote_url = 'https://www.senate.gov/'+ summary[0].find('a',href=True)['href']
                vote_page = requests.get(vote_url)
                vote_suop = BeautifulSoup(vote_page.text)
                votes = vote_suop.find('span', class_ = 'contenttext')        
                for i in range(100):
                    senate = votes.next.split(',')[0]
                    votes = votes.next.next
                    vote = votes.next
                    votes = votes.next.next.next
                    row = ','.join((senate,roll_call_id,vote))
                    vote_output.write(row + '\n') 
                                 
if __name__ == '__main__':
	main()