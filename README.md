# Wikipedia pageview

A Spark application to
- compute the top 25 pages on Wikipedia for each of the Wikipedia sub-domains
- write the output to S3

## Build the app

An already compiled JAR is provided in `build/libs/` but feel free to build it for yourself
```bash
./gradlew clean build
```
Note: this command will overwrite the provided JAR.

## Run the app

### App usage
```
Usage: wikipedia.PageViewApp [options]
  -s, --start-date <value>  The (included) start date as yyyy-MM-dd-HH in UTC. Defaults to the previous day and hour
  -e, --end-date <value>    The (excluded) end date as yyyy-MM-dd-HH in UTC. Defaults to (start-date + 1h)
  -b, --bucket-name <value> The output S3 bucket name. Required.
```

### AWS authentication
Since the app writes the output file(s) to S3, it has to authenticate to your AWS account.
You may use any credential provider supported by the AWS cli (see the [AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#config-settings-and-precedence)), such as a named profile and the `AWS_PROFILE` env var.

You also need to have the proper IAM permissions on the given S3 bucket.

### Run locally as a standalone JAR
**Requires Java 1.8**

As any Scala/Java Spark app, you can run it directly as a JAR.
The environment variable `SPARK_LOCAL_RUN` should be set to `true`
```bash
SPARK_LOCAL_RUN=true java -cp build/libs/wikipedia-pageview-1.0.0.jar wikipedia.PageViewApp -s 2020-06-01-08 -b my-bucket
```

### Run on a Spark cluster
You can also run it in your favorite Spark environment, using a `spark-submit` command
```bash
spark-submit --class wikipedia.PageViewApp wikipedia-pageview-1.0.0.jar -s 2020-06-01-08 -b my-bucket
```

Note: the application uses _Hadoop 2.8.5_ and _Spark 2.4.5_ dependencies.
It was successfully tested on Databricks Runtime Version 6.6 and on EMR Release 5.30.0
