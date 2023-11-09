FROM python:3.12

WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

ENV NAME hkjobsb

CMD ["scrapy", "runspider", "hkjobsdb.py", "-O", "hkjobsdb.csv"]
