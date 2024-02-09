import requests
from bs4 import BeautifulSoup
import sqlite3
import time

# Database setup
conn = sqlite3.connect('scraped_data.db')
c = conn.cursor()
# Create table
c.execute('''CREATE TABLE IF NOT EXISTS data (content TEXT)''')
conn.commit()

def scrape_site(url, session):
    try:
        response = session.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Modify below to extract the data you need
            for data_point in soup.find_all('div', class_='data-class'):  # Example class
                content = data_point.get_text(strip=True)
                if content:
                    c.execute('INSERT INTO data (content) VALUES (?)', (content,))
            conn.commit()
            # Respectful delay between requests
            time.sleep(1)
        else:
            print(f"Failed to retrieve {url}: Status code {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Use a session for efficient connection pooling
session = requests.Session()

# Starting URL provided by the site owner
starting_url = 'http://marcella.ai'

# Start the scraping process
scrape_site(starting_url, session)

# Close the database connection when done
conn.close()
