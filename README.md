Week 10: Airflow Data Pipeline (NYC Taxi Parquet)This project demonstrates a complete, end-to-end data pipeline orchestrated with Apache Airflow using real-world data. It fulfills the assignment requirements by:Running Airflow in a containerized environment using Docker.Ingesting two related, real-world datasets in Parquet format (Yellow & Green cab data for Jan 2024).Transforming the datasets in parallel using an Airflow TaskGroup.Combining the two datasets and writing the result to an intermediate Parquet file.Loading the final combined data into a dedicated PostgreSQL database.Performing two parallel analysis tasks:One that reads from the Postgres DB to generate a visualization.A "Super Bonus" task that uses PySpark to read the combined Parquet file.Cleaning up all intermediate data files.Project Structure.
├── dags/
│   ├── data/
│   │   ├── yellow_tripdata_2024-01.parquet  <-- (You must download this)
│   │   └── green_tripdata_2024-01.parquet   <-- (You must download this)
│   └── pipeline_dag.py
├── .devcontainer/
│   └── devcontainer.json
├── .env
├── docker-compose.yml
└── Dockerfile

🚀 How to Run1. PrerequisitesDocker and Docker Compose(Optional) VS Code with the Dev Containers extension.2. ⚠️ IMPORTANT: Get The DataThis pipeline will not run without the data.Go to the NYC TLC Trip Record Data Website.Scroll down to the 2024 section.Download the Yellow Taxi Trip Records for January (yellow_tripdata_2024-01.parquet).Download the Green Taxi Trip Records for January (green_tripdata_2024-01.parquet).Place both of these files into the dags/data/ directory.3. Initial SetupCreate Directories: Manually create the dags/data and .devcontainer directories.Save Files: Place all the files from this response into their correct locations.Initialize Airflow:docker-compose up airflow-init

Start Services:docker-compose up

Access the UI at http://localhost:8080 (login: airflow / airflow).4. Add Airflow ConnectionYou must add the connection for the pipeline's database:In the Airflow UI, go to Admin -> Connections.Add a new connection with these exact values:Connection Id: pipeline_postgres_connConnection Type: PostgresHost: pipeline-postgresSchema: publicLogin: pipeline_userPassword: pipeline_passwordPort: 5432Database: pipeline_dbClick Test, then Save.5. Run the PipelineIn the Airflow UI, un-pause the week_10_pipeline DAG.Trigger the DAG using the "Play" button (▶).Check the logs for the perform_db_analysis and perform_spark_analysis tasks to see their output.