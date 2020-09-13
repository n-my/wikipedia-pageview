### What additional things would you want to operate this application in a production setting?

Apart from the application itself, here are a few things I would want in order to operate it in a production setting:
* a staging and a production environments. Whatever these environments are, their creation should be fully automated
* a CI pipeline: automate the release process, as code
* a CD pipeline: automate the deployments to staging and to prod, as code
* a monitoring service
* a log management system: centralize the logs, make them searchable
* an alerting system: the team should be alerted if the job fails, takes too long, etc

### What might change about your solution if this application needed to run automatically for each hour of the day?

This repository only contains the Spark application. If it needed to run automatically for each hour of the day,
we would need to a workflow orchestrator such as Luigi or Apache Airflow to define
and operate a multi-step pipeline.

Using this pipeline, we could remove a couple of steps from the application to make it idempotent:
* do not download the input files over HTTP during the Spark application but have a pre-step downloading the file to S3
* remove the mechanism that prevents from reprocessing already processed hours. This should be part of the data pipeline.
Then have the application to write in an overwriting mode, so it always has the same output for the same input.

### How would you test this application?

I would test it in 2 ways:
* unit testing: increase code coverage, especially for the business logic parts of the code to gain confidence not 
to break logic when updating it
* end to end testing: this is why we need a staging environment. Batch jobs such as this one are easy to test before
releasing a new version. Keep running v1 in prod and run v2 in staging, then automatically compare their outputs
for a few hours or days

### How you’d improve on this application design?

First of all, let's mention a few design decisions of the current design:

Why use Spark for such small datasets?
* Spark has a great API to read/write data from/to external systems in many formats
* Simple and efficient API to manipulate data
* If the input data scales, easy to handle

Why this `datetime=yyyy-MM-dd-HH` S3 key pattern?
* This pattern is Hive-style partitioning. It's useful to respect this pattern to have query engines such as Presto
automatically understanding the partitioning so we can query only the required partitions rather than the whole dataset.
* We opted for a single key partitioning such as `datetime=yyyy-MM-hh-dd` rather than multiple key partitioning
such as `year=yyyy/month=MM/day=dd/hour=HH` to have simpler SQL queries

I would improve this application design with
* we already mentioned pre-downloading the input files to S3 for idempotence. We should also do it for performance reasons:
    * parallelize the loading file to Spark step. Rather than having a single executor downloading the file over HTTP,
    reading it from S3 would be parallelized
    * no need to download the input files to all the Spark nodes. This is currently the case because of the `spark.addFile` calls
* use configuration files, one per environment. Currently, the application only uses program arguments and environment variables.
This would be useful to define environment specific properties, e.g. the bucket name, or the Spark master to `local[*]` for local runs
* report on invalid inputs. Currently, incorrect values are read as lines with null values. It would be interesting
to report on the proportion of invalid input lines (using Spark accumulators) and to fail the job if it is greater than a defined threshold.
* use a query friendly file format for the output files? We used CSV and gzip to be homogenous with the input files,
but we could consider file formats such as Parquet and compression codecs such as lz4
* process multiple hours in parallel rather than sequentially. Currently, when the application processes multiple hours,
it does so hour by hour with no other good reason than simplicity. If performance is an issue, we could parallelize it.

Finally, we could even consider replacing this whole application with a single SQL query with tools such as Presto and dbt.
Of course, this wouldn't be as flexible but very fast to develop. It all depends on the use case!
