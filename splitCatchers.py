import pandas as pd

class PositionSplitter:
    def __init__(self, input_csv, output_csv):
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.df = None
        self.new_df = None

    def load_data(self):
        self.df = pd.read_csv(self.input_csv)
    
    def splitCatcherFielder(self):
        catcher_rows = []
        fielder_rows = []
        
        for _, row in self.df.iterrows():
            if pd.notna(row['run_value_fielding']) and row['run_value_catching'] != 0:
                # Create a copy for catcher stats
                catcher_row = row.copy()
                catcher_row['run_value'] = row['run_value_catching']
                catcher_row['run_value_fielding'] = 0
                catcher_row['run_value_range'] = 0
                catcher_row['run_value_arm'] = 0
                catcher_row['outs'] = row['outs_2']
                catcher_row['outs_3'] = 0
                catcher_row['outs_4'] = 0
                catcher_row['outs_5'] = 0
                catcher_row['outs_6'] = 0
                catcher_row['outs_7'] = 0
                catcher_row['outs_8'] = 0
                catcher_row['outs_9'] = 0
                catcher_rows.append(catcher_row)

                # Create a copy for fielder stats
                fielder_row = row.copy()
                fielder_row['run_value'] = row['run_value_fielding']
                fielder_row['run_value_catching'] = 0
                fielder_row['run_value_framing'] = 0
                fielder_row['run_value_stealing'] = 0
                fielder_row['run_value_blocks'] = 0
                fielder_row['outs'] -= row['outs_2']
                fielder_row['outs_2'] = 0
                fielder_rows.append(fielder_row)
            else:
                # If the player only played one role, keep the row as is
                if pd.notna(row['run_value_catching']):
                    catcher_rows.append(row)
                else:
                    fielder_rows.append(row)
        
        self.new_df = pd.concat([pd.DataFrame(catcher_rows), pd.DataFrame(fielder_rows)], ignore_index=True)

    def splitInfielderOutfielder(self):
        infielder_rows = []
        outfielder_rows = []
        other_rows = []
        for _, row in self.new_df.iterrows():
            infOuts = row['outs_3'] + row['outs_4'] + row['outs_5'] + row['outs_6']
            ofOuts = row['outs_7'] + row['outs_8'] + row['outs_9']
            if infOuts > 0 and ofOuts > 0:
                infielder_row = row.copy()
                infRatio = infOuts / (infOuts + ofOuts)
                infielder_row['outs'] = infOuts
                infielder_row['outs_7'] = 0
                infielder_row['outs_8'] = 0
                infielder_row['outs_9'] = 0
                infielder_row['run_value'] = row['run_value'] * infRatio
                infielder_row['run_value_fielding'] = row['run_value_fielding'] * infRatio
                infielder_row['run_value_range'] = row['run_value_range'] * infRatio
                infielder_row['run_value_arm'] = row['run_value_arm'] * infRatio
                infielder_rows.append(infielder_row)
        
                outfielder_row = row.copy() 
                ofRatio = 1 - infRatio
                outfielder_row['outs'] = ofOuts
                outfielder_row['outs_3'] = 0
                outfielder_row['outs_4'] = 0
                outfielder_row['outs_5'] = 0
                outfielder_row['outs_6'] = 0
                outfielder_row['run_value'] = row['run_value'] * ofRatio
                outfielder_row['run_value_fielding'] = row['run_value_fielding'] * ofRatio
                outfielder_row['run_value_range'] = row['run_value_range'] * ofRatio
                outfielder_row['run_value_arm'] = row['run_value_arm'] * ofRatio
                outfielder_rows.append(outfielder_row)
            else:
                other_rows.append(row)
        
        self.new_df = pd.concat([pd.DataFrame(infielder_rows), pd.DataFrame(outfielder_rows), pd.DataFrame(other_rows)], ignore_index=True)

    def save_data(self):
        self.new_df.to_csv(self.output_csv, index=False)
        print(f"New CSV file saved as {self.output_csv}")

# Example usage
# splitter = PositionSplitter('splitTesting.csv', 'splitTesting2.csv')
# splitter.load_data()
# splitter.splitCatcherFielder()
# splitter.splitInfielderOutfielder()
# splitter.save_data()
