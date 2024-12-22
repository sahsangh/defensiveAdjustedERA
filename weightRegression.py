import statsmodels.api as sm
import pandas as pd
import mysql.connector

def calculate_frv_impact_on_runs():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="team_run_values"
    )
    cursor = conn.cursor()

    # SQL query to join the tables and calculate the impact of FRV on runs allowed
    query = """
    SELECT t.team_id, t.ER, t.ERA, f.cumulative_run_value_fielding, f.position
    FROM team_era_earned_runs t
    JOIN team_run_values f ON t.team_id = f.team_id
    """

    # Execute the query and load results into a DataFrame
    result_df = pd.read_sql(query, con=conn)

    # Pivot the FRV data to have columns for each position ('catcher', 'infielder', 'outfielder')
    pivot_df = result_df.pivot_table(values='cumulative_run_value_fielding', index=['team_id', 'ER', 'ERA'], columns='position', fill_value=0).reset_index()

    # Rename columns for easier access
    pivot_df.columns.name = None
    pivot_df.rename(columns={'catcher': 'CatcherFRV', 'infielder': 'InfieldFRV', 'outfielder': 'OutfieldFRV'}, inplace=True)

    # Perform regression analysis to determine the impact of FRV on runs allowed
    

    # Independent variables (FRV values)
    X = pivot_df[['CatcherFRV', 'InfieldFRV', 'OutfieldFRV']]
    # Dependent variable (ER: Earned Runs)
    y = pivot_df['ER']

    # Add a constant to the independent variables for the regression intercept
    X = sm.add_constant(X)

    # Fit the regression model
    model = sm.OLS(y, X).fit()

    # Print the model summary to get the coefficients (weights)
    print(model.summary())

    # Create a new table to store the FRV weights in the database for future reference
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS frv_weights (
            position VARCHAR(255),
            weight FLOAT
        )
    ''')

    # Clear any existing data in frv_weights table
    cursor.execute('DELETE FROM frv_weights')

    # Insert the calculated weights into the new table
    cursor.execute('''
        INSERT INTO frv_weights (position, weight) VALUES (%s, %s), (%s, %s), (%s, %s)
    ''', ('catcher', model.params['CatcherFRV'], 'infielder', model.params['InfieldFRV'], 'outfielder', model.params['OutfieldFRV']))

    # Commit the changes to the database
    conn.commit()

calculate_frv_impact_on_runs()