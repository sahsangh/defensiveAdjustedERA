import requests
from bs4 import BeautifulSoup
import mysql.connector
import re

# Set up MySQL connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="team_run_values"
)
cursor = db.cursor()

# List of team names
team_names_str = "Mets, Pirates, Phillies, Diamondbacks, Braves, Orioles, Red Sox, White Sox, Cubs, Reds, Guardians, Rockies, Tigers, Astros, Royals, Angels, Dodgers, Marlins, Brewers, Twins, Yankees, Athletics, Padres, Giants, Mariners, Cardinals, Rays, Rangers, Blue Jays, Nationals"
team_names_list = [team.strip() for team in team_names_str.split(",")]

# URL setup
base_url = "https://mlb.com/"

# Loop through the team names to get their IDs
for team_name in team_names_list:
    url = f"{base_url}{team_name.lower().replace(' ', '')}"
    response = requests.get(url)

    # Check if request is successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the team id from the class containing "team-XXX"
        class_with_id = soup.find('body', class_=re.compile(r'team-\d+'))
        if class_with_id:
            team_id = re.search(r'team-(\d+)', ' '.join(class_with_id['class'])).group(1)

            # Insert data into the MySQL table
            insert_query = "INSERT INTO mlb_teams (team_id, team_name) VALUES (%s, %s)"
            cursor.execute(insert_query, (team_id, team_name))
            db.commit()

            print(f"Inserted team: ID={team_id}, Name={team_name}")
        else:
            print(f"Team ID not found for team {team_name}")
    else:
        print(f"Failed to fetch data for team {team_name}, Status code: {response.status_code}")

# Close the database connection
cursor.close()
db.close()
