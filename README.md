# Scraper for hk.jobsdb.com

The scraper uses python with the scrapy library.

## Installation
```sh
pip install scrapy
```

# Usage
```sh
scrapy runspider hkjobsdb.py -O hkjobsdb.csv 
```
If you want to log to a file instead of the console 

```sh
scrapy runspider hkjobsdb.py -O hkjobsdb.csv --logfile logs/log.txt
```


I found that the website uses an API request. So I copied the API request to get the data.

There are to API requests, one for the job search which returns all the jobs with pagination. I was able to get 
all the required fields except 'benefits' through this API request.

To get the benefits we need to make another request to the job detail api.


