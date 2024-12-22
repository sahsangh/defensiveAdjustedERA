import mysql.connector
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

# Connect to MySQL Database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='team_run_values'
)
cursor = conn.cursor()

def calculate_stat_for_pitchers(conn):
    cursor = conn.cursor()

    # SQL query to get all the necessary data for each pitcher
    query = """
    SELECT p.Name, p.ERA, p.SO, p.BB, p.GB, p.IFFB, p.LD, p.FB, p.TBF, 
           f_cumulative.CumulativeFRV AS CatcherFRV, f_infield.CumulativeFRV AS InfieldFRV, f_outfield.CumulativeFRV AS OutfieldFRV
    FROM pitching_stats_filtered p
    LEFT JOIN (
        SELECT team_id, cumulative_run_value_fielding AS CumulativeFRV
        FROM team_run_values
        WHERE position = 'catcher'
    ) f_cumulative ON p.team_id = f_cumulative.team_id
    LEFT JOIN (
        SELECT team_id, cumulative_run_value_fielding AS CumulativeFRV
        FROM team_run_values
        WHERE position = 'infielder'
    ) f_infield ON p.team_id = f_infield.team_id
    LEFT JOIN (
        SELECT team_id, cumulative_run_value_fielding AS CumulativeFRV
        FROM team_run_values
        WHERE position = 'outfielder'
    ) f_outfield ON p.team_id = f_outfield.team_id
    """

    # Execute the query and load results into a DataFrame
    result_df = pd.read_sql(query, con=conn)

    # Calculate the stat for each pitcher
    stats = []
    for _, row in result_df.iterrows():
        # Extract pitcher data
        ERA = row['ERA']
        SO = row['SO']
        BB = row['BB']
        GB = row['GB']
        IFFB = row['IFFB']
        LD = row['LD']
        FB = row['FB']
        TBF = row['TBF']
        CatcherFRV = row['CatcherFRV']
        InfieldFRV = row['InfieldFRV']
        OutfieldFRV = row['OutfieldFRV']

        # Weights (positive to indicate better defense leads to higher DAERA)
        SOWeight = 2.5266   # Positive weight for strikeout factor
        GBWeight = 0.468639 # Positive weight for ground ball factor
        FBWeight = 0.664169 # Positive weight for fly ball factor

        # Calculate factors
        SOFactor = ((SO / TBF) + (BB / TBF)) * CatcherFRV * SOWeight
        GBFactor = ((GB / TBF) + (IFFB / TBF) + (LD * 0.14 / TBF)) * InfieldFRV * GBWeight
        FBFactor = ((FB / TBF) - (IFFB / TBF) + (LD * 0.86 / TBF)) * OutfieldFRV * FBWeight

        # Calculate the stat
        stat_value = ERA + SOFactor + GBFactor + FBFactor

        # Append result for each pitcher
        stats.append({'Name': row['Name'], 'stat_value': stat_value})

    # Create a DataFrame for the calculated stats
    stats_df = pd.DataFrame(stats)

    # Drop rows with None values in 'stat_value'
    stats_df.dropna(subset=['stat_value'], inplace=True)

    # Print the calculated stats for verification
    print(stats_df)

    # Normalize the DAERA values to get DAERA+
    mean_value = stats_df['stat_value'].mean()
    print(f"MEAN: {mean_value}")
    stats_df = stats_df[stats_df['stat_value'].notna()]
    stats_df['DAERA+'] = stats_df['stat_value'].apply(lambda x: (x / mean_value) * 100)

    # Print the DAERA+ values for verification
    print(stats_df[['Name', 'DAERA+']])

    # Store the calculated stats in the database
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS final_DAERA_Values (
            Name VARCHAR(255),
            stat_value FLOAT,
            DAERA_plus FLOAT
        )
    ''')

    # Clear any existing data in final_DAERA_Values table
    cursor.execute('DELETE FROM final_DAERA_Values')

    # Insert the calculated stats into the new table
    for _, row in stats_df.iterrows():
        cursor.execute('''
            INSERT INTO final_DAERA_Values (Name, stat_value, DAERA_plus) VALUES (%s, %s, %s)
        ''', (row['Name'], row['stat_value'], row['DAERA+']))

    # Commit the changes to the database
    conn.commit()
    plt.hist(stats_df['stat_value'], bins=20, edgecolor='black')
    plt.xlabel('DAERA Value')
    plt.ylabel('Frequency')
    plt.title('Distribution of DAERA Values')
    plt.show()

# Example usage
calculate_stat_for_pitchers(conn)

# Close the connection
conn.close()
