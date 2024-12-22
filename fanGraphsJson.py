import requests
import json
import pandas as pd
import mysql.connector

# Fetch JSON data from the webpage
url = "https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats=pit&lg=all&qual=0&season=2024&season1=2024&startdate=2024-03-01&enddate=2024-11-01&month=0&hand=&team=0&pageitems=2000000000&pagenum=1&ind=0&rost=0&players=&type=2&postseason=&sortdir=default&sortstat=Hard%25"
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Load JSON data
    data = response.json()

    # Extract the 'data' list from the dictionary
    data_list = data["data"]

    # Convert list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Clean up HTML tags if needed (for 'Name' and 'Team' fields)
    if 'Name' in df.columns:
        df['Name'] = df['Name'].str.extract(r'>(.*?)<', expand=False)
    if 'Team' in df.columns:
        df['Team'] = df['Team'].str.extract(r'>(.*?)<', expand=False)

    # Replace NaN values with None (which translates to NULL in SQL)
    df = df.where(pd.notnull(df), None)

    # Set up MySQL connection
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="team_run_values"
    )
    cursor = db.cursor()

    # Create a new table to store the data
    table_name = "mlb_pitching_stats_2024"
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Define table creation query dynamically based on the DataFrame columns
    create_table_query = f"CREATE TABLE {table_name} (id INT PRIMARY KEY AUTO_INCREMENT, "
    
    for column in df.columns:
        # Enclose column names in backticks to handle special characters
        clean_column = f"`{column}`"
        sample_value = df[column].dropna().iloc[0] if not df[column].dropna().empty else ""

        # Determine the appropriate data type based on the sample value
        if isinstance(sample_value, int):
            create_table_query += f"{clean_column} INT, "
        elif isinstance(sample_value, float):
            create_table_query += f"{clean_column} FLOAT, "
        elif isinstance(sample_value, str):
            max_length = min(255, df[column].str.len().max() if df[column].str.len().max() else 255)
            create_table_query += f"{clean_column} VARCHAR({int(max_length)}), "
        else:
            create_table_query += f"{clean_column} VARCHAR(255), "

    # Removing the trailing comma and adding closing parenthesis
    create_table_query = create_table_query.rstrip(', ') + ")"

    # Execute table creation query
    cursor.execute(create_table_query)

    # Insert data into the MySQL table
    for _, row in df.iterrows():
        columns = ', '.join([f'`{col}`' for col in df.columns])
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
