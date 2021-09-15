# Jobsdb-All-Jobs-Scrapper
It scrap all jobs on jobsdb.com

# Scraper for hk.jobsdb.com

The scraper uses python with the scrapy library.

## Installation
```sh
pip insall scrapy
```

# Usage
```sh
scrapy runspider hkjobsdb.py -O hkjobsdb.csv 
```

I found that the website uses an API request. So I copied the API request to get the data.

There are to API requests, one for the job search which returns all the jobs with pagination. I was able to get 
all the required fields except 'benefits' through this API request.

To get the benefits we need to make another request to the job detail api.


