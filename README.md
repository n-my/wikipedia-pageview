# wikipedia-pageview-data-pipeline

Build a simple application that we can run to compute the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:

    - Accept input parameters for the date and hour of data to analyze (default to the current date/hour - 24 hours if not passed, i.e. previous day/hour).
    - Download the page view counts from wikipedia for the given date/hour from https://dumps.wikimedia.org/other/pageviews/
        More information on the format can be found here: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
    - Eliminate any pages found in this blacklist: https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages
    - Compute the top 25 articles for the given day and hour by total pageviews for each unique domain in the remaining data.
    - Save the results to a file, either locally or on S3, sorted by domain and number of pageviews for easy perusal.
    - Only run these steps if necessary; that is, not rerun if the work has already been done for the given day and hour.
    - Be capable of being run for a range of dates and hours; each hour within the range should have its own result file.

For your solution, explain:

    - What additional things would you want to operate this application in a production setting?
    - What might change about your solution if this application needed to run automatically for each hour of the day?
    - How would you test this application?
    - How you’d improve on this application design?
    
SPARK_LOCAL_RUN