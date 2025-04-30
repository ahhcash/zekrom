# Write-up questions

1. I spent a total of 5-6 hours on the problem (according to my according to my IDE plugin :)). The major difficulties I ran into was properly understanding how grib2 files are configured and 
different parameters for all the target variables. The documentation is all over the place without much clarity. It was kinda difficult to identify exactly what I was looking for. Apart from that, 
it was identifying the right library to use. I originally intended to build this in Go, but there isn't much library support available, so I pivoted tom using Python instead. 
Another *major* challenge was figuring out who to use eccodes to read directly from s3 (as a network stream).Based on what I could find, eccodes is a file based tool (as it uses file handles underneath 
to inspect grib2 files). I tried to read directly from s3 and for some reason, the whole process wrecked my internet connection and started erroring out (because `boto3` returns a network stream of 
bytes for each file which will not have a file handle that `eccodes` expects). I decided to just download these files to a temp directory, process them and clean them up.

2. The only assistant I used was Google Gemini 2.5 pro, through Google AI studio and Windsurf. Here are some of the prompts, I used,
    -
    ```i have a grib2 file and it's corresponding grib2.idx file in plaintext. hrrr.t06z.wrfnatf15.grib2 and hrrr.t06z.wrfnatf15.grib2.idx. i want to read the entire contents of this file and print it out stdout. i'm doing this in golang, and found this librayr that would help. https://github.com/amsokol/go-grib2. 
   help me write some boilerplate code for this. take it step by step, don't do too much at once. before moving on to the next step, confirm with me.
   ```
    - 
    ```okay, this worked. i have a few questions.
    why did i get the fileno error earlier then?
    what is different about downloading the file and then processing it as opposed to prcoessing on s3 (like we did earlier)
    in each of these grib2 files's messages (records), are there any indications of latitudes and longitudes?
   ```
    -
    ```so for every message in a single forecast run, will the grid ID that im constructing be the same? basically what i'm asking is, is the grid caching method logically sound?
   ```
   -
   ```okay, now on to the next major task. i want to refactor the code into  a maintainable project structure. can you help me do this? currently, all of the logic is in the single file you see - main.py. i want to make it structured and maintainable. let's do this one step at a time. once im finished with a step, 
   i'll tell you to move to the next one. understood?
   ```

    The main areas where it helped was refactoring and packaging everything into a command line tool.

3. Deploying this solution as a production service would involve several key components:
    *   **Deployment & Orchestration:** I would containerize the application using Docker. For orchestration and scheduling, I would use a cloud-native workflow tool like Prefect Cloud or Apache Airflow. This allows defining the ingestion process as a DAG, clearly separating steps like downloading, processing, and storing. These tools provide robust scheduling (including CRON for regular intervals), automatic retries for transient failures, parallel execution, and visibility into pipeline runs. Deployment of the orchestration engine and the containerized workers could be managed using Kubernetes (like AWS EKS or GKE) for scalability and resilience. CI/CD pipelines (e.g., using GitHub Actions or GitLab CI) would automate testing and deployment.
    *   **Scheduling Ingestion:** For scheduling, the chosen workflow orchestrator (Prefect/Airflow) would handle time-based schedules (e.g., checking for new HRRR runs every hour based on their known release times). To handle data availability more dynamically, I'd supplement this with event-driven triggers. For instance, an AWS S3 event notification could trigger a Lambda function or send a message to a queue when a new `.idx` file appears, which in turn would trigger the Prefect/Airflow DAG run for that specific forecast cycle. This ensures data is processed as soon as it's available.
    *   **Data Storage:** To make the processed data readily available for analysts and researchers, I would store the final, processed data (the extracted points in a tabular format) in a cloud data warehouse like Google BigQuery, AWS Redshift, Snowflake or even Motherduck (since we're using DuckDB). These platforms are optimized for analytical queries and integrate well with BI tools and data science environments (like Jupyter notebooks). The data would likely be stored in an efficient columnar format like Parquet before loading. This structure allows analysts to easily query data across different forecast times, variables, and locations without needing to understand the complexities of GRIB2 files.
    *   **Monitoring:** Monitoring can be introduced in a few places.
        *   **Workflow Monitoring:** Utilize the dashboards provided by Prefect/Airflow to monitor DAG success/failure rates, task durations, and logs. Prefect cloud is *very* good at this.
        *   **Infrastructure Monitoring:** Use cloud provider tools (like AWS CloudWacth) or platforms like Datadog to monitor the health of the underlying infrastructure (Kubernetes pods, VMs, serverless functions) - tracking CPU, memory, network, and disk usage.
        *   **Application Monitoring:** Integrate observability tools (e.g., Datadog, Sentry for error tracking) within the processing code to trace requests, identify bottlenecks, and capture application-level errors.
        *   **Alerting:** Configure alerts (via PagerDuty, Slack, or email) based on critical events like pipeline failures, significant increases in error rates, resource exhaustion, or data quality check failures.

4. If we use a workflow orchestrator like Prefect or Airflow, we can define parameterized workflows, i.e, workflows for specific dates / hours. After identifying all histroical forecast runs that need backfillimg, we can trigger massively parallel workflow jobs that can take different parameters. This would efficiently backfill a large amount of historical data.
Both Airflow and Prefect have kubernetes support (via Prefect Agents and K8s Operators) which allow for completely managed horizontal autoscaling. This would also help with each individual DAG run. Beyond that, it would also help to implement idempotency for each ingestion task, much like how we already introduce it for the final processed data. This ensures that DAG runs that are accidentally run twice are pre-empted before being executed.
The main improvements I would make, is to introduce Prefect tasks for each of the major functions within the ingestion logic (likely found in a file like `ingest.py`, covering steps such as downloading, processing GRIB files, extracting points, and saving data). By decorating these Python functions with `@task` from Prefect, each step becomes an independently schedulable unit within a larger Prefect `@flow`. For large backfills, where we trigger
parameterized flows for numerous historical dates/times, Prefect's concurrency capabilities are suoper useful. When deployed with a Kubernetes-based worker (Prefect 2+), Prefect can submit each task run (or even the entire flow run) as a Kubernetes Job. This means downloading and processing different forecast cycles can happen entirely in parallel, each within its own dedicated Kubernetes Pod. Furthermore, by setting up Horizontal Pod Autoscaling (HPA) on the Kubernetes cluster targeting the Prefect Agent or worker deployment, the cluster can automatically scale the number of worker pods up to handle the surge of tasks during a backfill, and scale down later to save resources. This combination provides both the logical parallelization via Prefect tasks and the physical resource scaling via Kubernetes.
