## ➡️ Project Goal:
Connect to an API to collect raw data, transform it using dbt, store it in Google BigQuery (data warehouse), and version everything using GitHub.

## Data Source: 
NYC Open Data
- Dataset: NYC Motor Vehicle Collisions

## Tools Used:
- SQL
- Python
- Git
- BigQuery
- dbt
- Visual Studio Code
- Tableau - For Visualization


## ✳️ Steps I followed:
- Initialized a dbt project in VS Code.
- Created a project file and a dataset in BigQuery to host raw data.
- Configured profiles.yml to connect dbt to BigQuery.
- Run dbt debug to validate the setup.
- Built a Python ingestion script to fetch 100,000 data from the NYC Open Data API.
- Loaded raw data into BigQuery.
- Created a GitHub repository and linked it to my local project
- Tracked and pushed changes using Git.

## ✳️ Visualization:
- Connected Tableau to the NYC Motor vehicle collision Project in Google BigQuery.
- Built a dashboard to show the Total Crashes, Total Casualties, Total Death Toll, Total Injured Toll, Contributing factors leading to Crashes, Most Vehicle types involved and a heatmap on the Crashes per location.

## Dashboard
<img width="2451" height="1564" alt="Tableau - NYC Motor Crashes 6_2_2026 00_40_05" src="https://github.com/user-attachments/assets/b67387aa-48e7-4661-820e-6f565075cc30" />
