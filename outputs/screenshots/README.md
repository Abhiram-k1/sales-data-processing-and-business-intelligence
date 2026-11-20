# Capture the required Spark UI and DAG screenshots

Run `python main.py --sample --ui-hold-seconds 300` and open the printed Spark UI URL, normally http://localhost:4040. Spark selects another port if 4040 is busy. The application stays active for 300 seconds after outputs finish.

1. Open **Jobs** and save a screenshot of the completed analytical jobs as `spark_ui_jobs.png`.
2. Click a job that aggregates category revenue, expand **DAG Visualization**, and save `spark_dag.png`.
3. Open **SQL / DataFrame**, choose an aggregate query, and save its physical execution diagram as `spark_sql_plan.png`.
4. Open **Stages** or **Executors** to inspect task counts, shuffle, partitions, and executor activity. Save additional screenshots if required by your instructor.

Use actual browser screenshots. Include the application name, selected job/query, and readable diagram. Explain the displayed Exchange/shuffle, aggregate operators, and task counts using `docs/methodology.md`. Screenshot files are a manual deliverable unless actual captured evidence is supplied with this project.

After the application stops, launch a Spark 3.5 History Server with `spark.history.fs.logDirectory` pointing to the absolute `outputs/spark-events` directory. Open its printed UI, normally http://localhost:18080, and inspect the recorded jobs. On a Spark installation, set `SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=file:///absolute/path/to/outputs/spark-events` before running `sbin/start-history-server.sh` in Linux/WSL. The event log retains execution evidence, while screenshots still require capture from the UI.
