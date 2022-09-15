# Student Challenge Solutions
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

### Chest Symptom Ranking
**Problem:**  
The main dataset contains a column of symptoms that are aggregated into a single string column using "|" as a delimiter. We want a column to identify the most severe symptom for each row, and create a column of intergers that we can easily filter by severity. The secondary dataset contains a serverity ranking for each individual symptom.

**Solution:**  
1) Using SQL, I'm first going to add a unique ID to the main data, which contains the aggregated column in order to track my symptoms.
2) I first have a nested query on the main data and split the symptoms apart which pairs each symptom with it's corresponding  unique ID.
3) The following query then joins the two tables together on the symptom names which then gives a ranking to the main data rows for each individual symptom in the aggregated string.
4) With that new table, I'm using a groupby query in order to retrieve the aggregated symptoms highest severity rank which I then join to the main data on the original aggregated symptoms to create a final table which contains the column for the highest severity rank for each row.
```
ALTER TABLE main_data ADD COLUMN id SERIAL PRIMARY KEY;

WITH split_table (name_group, id, part, single_name, ranking)
	AS 
	(
		SELECT *
		FROM (
			SELECT 
				md.name_group, 
				md.id,
				unnest(string_to_array(md.name_group, ' | ')) AS part
			FROM main_data AS md
			) AS split
		INNER JOIN rank_data AS rd
		ON (split.part = rd.single_name)
	)
SELECT 
	md.name_group, 
	md.id, 
	rd.ranking, 
	rd.single_name
FROM 
(
	SELECT name_group, max(ranking) AS ranks
	FROM split_table
	GROUP BY name_group
) AS mr
INNER JOIN main_data as md
ON md.name_group = mr.name_group
INNER JOIN rank_data AS rd
ON ranks = rd.ranking
ORDER BY id ASC
```
