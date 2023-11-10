import json
import math
import scrapy
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

class HkJobsDbSpider(scrapy.Spider):
    name = 'hkjobsdb'
    total_pages = None
    job_per_page = 30
    handle_httpstatus_list = [400, 403, 404, 500]
    max_retries = 5
    retry_delay = 0.133

    custom_settings = {
        'RETRY_TIMES': max_retries,
        'RETRY_HTTP_CODES': handle_httpstatus_list,
        'DOWNLOAD_DELAY': retry_delay
    }

    def start_requests(self):
        """
        Send the initial request to the first page of the API. And also gets the total number of jobs and jobs per page
        so that we can calculate the total number of pages.
        The site was found using a graphql API. We can see the API request being made in the networks tab of the
        developer tools of any browser.
        """
        yield scrapy.Request("https://xapi.supercharge-srp.co/job-search/graphql?country=hk&isSmartSearch=true",
                             method="POST",
                             body=json.dumps({
                                 "query": "query getJobs{jobs(page: 1, locale: \"en\"){total jobs{id "
                                          "companyMeta{name}jobTitle "
                                          "jobUrl employmentTypes{name}categories{name}careerLevelName "
                                          "qualificationName industry{name}workExperienceName}}}",
                             }),
                             headers={
                                 'content-type': 'application/json',
                             },
                             callback=self.parse, 
                             errback=self.handle_error,
                            )

    def parse(self, response, **kwargs):
        """
        This function parses each page of the search result.
        When parsing the first page, it also gets the total jobs and calculates the last page.
        Then after parsing each page it sends a request to the next page.
        """
        self.logger.info(f"Received response status: {response.status} for URL: {response.url}")
        if response.status in self.handle_httpstatus_list:
            self.logger.info("Handling error...")
            self.handle_error(response)
            return
        page = response.meta.get("page", 1)

        # The response is a json string which we parse into python dictionaries and arrays
        dr = response.json()
        # The structure of the response matches the structure of the graphQl query (see file 'jobs.graphql')
        jobs = dr["data"]["jobs"]["jobs"]
        for job in jobs:
            item = {
                "job_id": job.get("id"),
                "job_title": job.get("jobTitle"),
                "company": job.get("companyMeta", {}).get("name"),
                "job_function": ", ".join([category["name"] for category in job.get("categories", [])]),
                "job_type": ", ".join(
                    [employment_type.get("name") for employment_type in job.get("employmentTypes", [])]),
                "industry": job.get("industry", {}).get("name"),
                "career_level": job.get("careerLevelName"),
                "years_of_experience": job.get("workExperienceName"),
                "qualification": job.get("qualificationName")
            }
            job_id = job.get("id")
            yield scrapy.Request(
                url="https://xapi.supercharge-srp.co/job-search/graphql?country=hk&isSmartSearch=true",
                callback=self.parse_detail,
                method="POST",
                body=json.dumps({
                    "query": "query getJobDetail{jobDetail(jobId: \"%s\", locale: \"en\", country: \"hk\")"
                             "{jobDetail {jobRequirement {benefits}}}}" % job_id
                }),
                headers={
                    'content-type': 'application/json',
                },
                meta={
                    "item": item
                },
                errback=self.handle_error
            )

        if self.total_pages is None:
            total_jobs = dr["data"]["jobs"]["total"]
            self.total_pages = math.ceil(total_jobs / self.job_per_page)
        if page < self.total_pages:
            page += 1
            yield scrapy.Request("https://xapi.supercharge-srp.co/job-search/graphql?country=hk&isSmartSearch=true",
                                 method="POST",
                                 body=json.dumps({
                                     "query": "query getJobs{jobs(page: %s, locale: \"en\"){total jobs{id "
                                              "companyMeta{name}jobTitle "
                                              "jobUrl employmentTypes{name}categories{name}careerLevelName "
                                              "qualificationName industry{name}workExperienceName}}}" % page,
                                 }),
                                 headers={
                                     'content-type': 'application/json',
                                 },
                                 meta={
                                     "page": page
                                 })

    def parse_detail(self, response):
        """
        This function parses the response from the job detail API request's response.
        We only need the benefits from here.
        """
        if response.status in self.handle_httpstatus_list:
            self.logger.info(f"Received response status: {response.status} for URL: {response.url}")
            self.handle_error(response)
            return
        item = response.meta.get("item")
        dr = response.json()
        job = dr.get("data").get("jobDetail")
        item["benefits"] = ", ".join(job.get("jobDetail").get("jobRequirement").get("benefits"))
        yield item

    def handle_error(self, response):
        """
        Handle failed requests and store the response in a different file.
        """
        self.logger.error(f"Request failed with status {response.status} for URL: {response.request.url}")
        self.logger.info(response.request.body.decode('utf-8'))

        error_data = {
            'url': response.request.url,  
            'status': response.status,
            'body': response.text
        }
        file_name = 'failed_requests.json'
        
        self.retry(response)

    def retry(self, failure):
        """
        Retry failed requests with a delay.
        """
        retry_times = failure.request.meta.get('retry_times', 0) + 1
        if retry_times <= self.max_retries:
            self.logger.error(f"Retrying URL: {failure.request.url} (attempt {retry_times})")
            retry_req = failure.request.copy()
            retry_req.dont_filter = True
            retry_req.meta['retry_times'] = retry_times
            retry_req.meta['download_slot'] = self.crawler.engine.downloader._get_slot_key(retry_req, None)
            self.crawler.engine.schedule(retry_req, self)
        else:
            self.logger.error(f"Giving up on URL: {failure.request.url} after {retry_times} attempts")
            try:
                with open(file_name, 'a') as file:
                    file.write(json.dumps(error_data) + "\n")
                    file.flush()  
            except Exception as e:
                self.logger.error(f"Failed to write to {file_name}: {e}")
