import requests
import json
import pandas as pd
import mysql.connector

# Fetch JSON data from the new API endpoint
url = "https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=pit&lg=all&qual=0&season=2024&season1=2024&startdate=2024-03-01&enddate=2024-11-01&month=0&hand=&team=0%2Cts&pageitems=30&pagenum=1&ind=0&rost=0&players=&type=0&postseason=&sortdir=default&sortstat=SO"
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Load JSON data
    data = response.json()

    # Extract the 'data' list from the dictionary
    data_list = data["data"]

    # Convert list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Clean up HTML tags if needed (for 'Team' fields)
    if 'Team' in df.columns:
        df['Team'] = df['Team'].str.extract(r'>(.*?)<', expand=False)

    # Extract only the required columns: 'Team', 'ERA', 'ER'
    filtered_df = df[['Team', 'ERA', 'ER']]

    # Replace NaN values with None (which translates to NULL in SQL)
    filtered_df = filtered_df.where(pd.notnull(filtered_df), None)

    # Set up MySQL connection
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="team_run_values"
    )
    cursor = db.cursor()

    # Create a new table to store the filtered data
    table_name = "team_era_earned_runs"
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Define table creation query for the filtered data
    create_table_query = f"""
    CREATE TABLE {table_name} (
        id INT PRIMARY KEY AUTO_INCREMENT,
        Team VARCHAR(255),
        ERA FLOAT,
        ER INT
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the MySQL table
    for _, row in filtered_df.iterrows():
        columns = ', '.join([f'`{col}`' for col in filtered_df.columns])
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Ensure all `NaN` values are replaced by `None` in the row
        cleaned_row = [None if pd.isna(val) else val for val in row]
        
        cursor.execute(insert_query, tuple(cleaned_row))

    # Commit changes
    db.commit()

    print(f"Data has been successfully added to the {table_name} table.")

    # Close the database connection
    cursor.close()
    db.close()

else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
