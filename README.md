# Student Solutions
Repository dedicated to solutions to various "challenges" that students have encountered in class.

### Baseball Data:
**Problem:**  
Main dataset contains partial team names (ie. Angels, Astros, Brewers...), and all other data contains franchise ID. We need a column to join tables on.

**Solution:**  
Using a new dataset which contains both Full Team Names & Franchise ID, I'm joining the partial name column to the full name column in order to add the franchise ID column to the main dataset for later SQL queries.

```
import pandas as pd

  # Find all the unique Team Names (shortened)
drafts = pd.read_csv('AllFirstRoundDrafts.csv')['Tm'].unique()
  # Filter via
drafts = [i if ' via ' not in i else i[:i.index(' via ')] for i in drafts]
  # Dataframe for shortened names
short_names = pd.DataFrame(pd.Series(drafts).unique(), columns=['shortname'])
  # Dataframe with the franchise IDs that are still active
long_names = pd.read_csv('team_franchise.csv').query('active == "Y"')[['franchise_id', 'franchise_name']]

  # Function to pair partial names to full names and create a column to join data on
def create_join_column(row: pd.Series):
    try:
        row['join_on'] = long_names.query('franchise_name.str.contains(@row.shortname)', engine='python').index[0]
    except:
        row['join_on'] = -1
    return row
  #  Apply the function
short_names = short_names.apply(create_join_column, axis=1).set_index('join_on', drop=True)
  # Join the two datasets
bridge_df = short_names.join(long_names).reset_index(drop=True)
print(bridge_df)
```

