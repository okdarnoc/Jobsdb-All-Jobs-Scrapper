import json
import math

import scrapy
import time

class HkJobsDbSpider(scrapy.Spider):
    name = 'hkjobsdb'
    total_pages = None
    job_per_page = 30
    handle_httpstatus_list = [400, 403, 404, 500] 

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
                             })

    def parse(self, response, **kwargs):

        if response.status in self.handle_httpstatus_list:
            self.handle_error(response)
            return
        """
        This function parses each page of the search result.
        When parsing the first page, it also gets the total jobs and calculates the last page.
        Then after parsing each page it sends a request to the next page.
        """
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
                }
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
        if response.status in self.handle_httpstatus_list:
            self.handle_error(response)
            return
        """
        This function parses the response from the job detail API request's response.
        We only need the benefits from here.
        """
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

        error_data = {
            'url': response.request.url,  
            'status': response.status,
            'body': response.text
        }
        file_name = 'failed_requests.json'
        with open(file_name, 'a') as file:
            file.write(json.dumps(error_data) + "\n")

        retry_times = response.meta.get('retry_times', 0) + 1
        if retry_times <= 2:
            self.logger.error(f"Retrying URL: {response.request.url} (attempt {retry_times})")
            retry_req = response.request.copy()
            retry_req.dont_filter = True 
            retry_req.meta['retry_times'] = retry_times
            retry_req.callback = response.request.callback
            yield retry_req
        else:
            self.logger.error(f"Giving up on URL: {response.request.url} after {retry_times} attempts")
