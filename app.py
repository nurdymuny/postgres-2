import os
import requests
from bs4 import BeautifulSoup
import psycopg2
import time

# Environment variables for database connection
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')  # Default to localhost if not set
DB_PORT = os.getenv('POSTGRES_PORT', '5432')  # Default to 5432 if not set

# Database setup using psycopg2
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE IF NOT EXISTS data (content TEXT)''')
conn.commit()

def scrape_site(url, session):
    try:
        response = session.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for data_point in soup.find_all('div', class_='data-class'):  # Modify this class
                content = data_point.get_text(strip=True)
                if content:
                    c.execute('INSERT INTO data (content) VALUES (%s)', (content,))
            conn.commit()
            time.sleep(1)  # Be respectful in request timing
        else:
            print(f"Failed to retrieve {url}: Status code {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Use a session for efficient connection pooling
session = requests.Session()

# Starting URL
starting_url = 'http://marcella.ai'

# Start the scraping process
scrape_site(starting_url, session)

# Close the database connection when done
conn.close()
