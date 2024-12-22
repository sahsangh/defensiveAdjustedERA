import mysql.connector
import pandas as pd
from splitCatchers import PositionSplitter

class TeamRunValueCalculator:
    def __init__(self, input_csv, output_csv):
        self.splitter = PositionSplitter(input_csv, output_csv)
        self.conn = None

    def process_data(self):
        # Load and split the data using PositionSplitter
        self.splitter.load_data()
        self.splitter.splitCatcherFielder()
        self.splitter.splitInfielderOutfielder()
        self.splitter.save_data()

        # Create a MySQL connection
        self.conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='team_run_values'
        )

        # Create a cursor to create the table and insert data
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                player_id INT,
                player_name VARCHAR(255),
                year INT,
                team_id VARCHAR(255),
                run_value FLOAT,
                run_value_fielding FLOAT,
                run_value_range FLOAT,
                run_value_arm FLOAT,
                run_value_catching FLOAT,
                run_value_framing FLOAT,
                run_value_stealing FLOAT,
                run_value_blocks FLOAT,
                outs INT,
                outs_2 INT,
                outs_3 INT,
                outs_4 INT,
                outs_5 INT,
                outs_6 INT,
                outs_7 INT,
                outs_8 INT,
                outs_9 INT
            )
        ''')

        # Replace NaN with None
        self.splitter.new_df = self.splitter.new_df.where(pd.notnull(self.splitter.new_df), None)
        cursor.execute('''DELETE FROM player_stats''')
        # Insert rows into MySQL
        for _, row in self.splitter.new_df.iterrows():
            # Replace NaN with None explicitly in each row
            row_values = [None if pd.isna(value) else value for value in row]

            cursor.execute('''
                INSERT INTO player_stats (
                    player_id, player_name, year, team_id,
                    run_value, run_value_fielding, run_value_range,
                    run_value_arm, run_value_catching, run_value_framing,
                    run_value_stealing, run_value_blocks, outs, outs_2,
                    outs_3, outs_4, outs_5, outs_6, outs_7, outs_8, outs_9
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', tuple(row_values))

        # Commit the changes to the database
        self.conn.commit()

    def calculate_team_run_values_sql(self):
            cursor = self.conn.cursor()

            # SQL query to calculate cumulative run_value_fielding for each team and position
            query = """
            SELECT team_id,
                CASE
                    WHEN run_value_catching != 0 THEN 'catcher'
                    WHEN outs_3 + outs_4 + outs_5 + outs_6 > 0 THEN 'infielder'
                    ELSE 'outfielder'
                END AS position,
                SUM(run_value) AS cumulative_run_value_fielding
            FROM player_stats
            GROUP BY team_id, position
            """

            # Execute the query and load results into a DataFrame
            result_df = pd.read_sql(query, con=self.conn)
            result_df = result_df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})
            # Create a new table to store the aggregated results
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS team_run_values (
                    team_id VARCHAR(255),
                    position VARCHAR(255),
                    cumulative_run_value_fielding FLOAT
                )
            ''')

            # Insert the aggregated values into the new table
            cursor.execute('''DELETE FROM team_run_values''')
            for _, row in result_df.iterrows():
                cursor.execute('''
                    INSERT INTO team_run_values (team_id, position, cumulative_run_value_fielding)
                    VALUES (%s, %s, %s)
                ''', tuple(row))

            self.conn.commit()
            # Print the result for verification
            print(result_df)

    def view_database(self):
        # View tables using an SQL query
        query = "SHOW TABLES;"
        tables_df = pd.read_sql_query(query, self.conn)
        print("Tables in the database:")
        print(tables_df)

    def close_connection(self):
        if self.conn:
            self.conn.close()

# Example usage
calculator = TeamRunValueCalculator('./dataFiles/fielding_run_value.csv', './dataFiles/allDataPostSplit.csv')
calculator.process_data()
calculator.calculate_team_run_values_sql()
calculator.view_database()
calculator.close_connection()
