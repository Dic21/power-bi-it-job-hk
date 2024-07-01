import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import math
import time
import pymongo
from config import DB_PASSWORD

today = datetime.now().date().strftime('%Y-%m-%d')


def reading_info(item):
    if item != None:
        desc_list = []
        for i in item:
            desc_list.append(i.text)
        delimiter = ', '
        my_string = delimiter.join(desc_list)
        return my_string
    else:
        return ""

def insertToMongo(df):
    try:
        #df.reset_index(inplace=True)
        data_dict = df.to_dict("records")

        myclient = pymongo.MongoClient(f"mongodb+srv://root:{DB_PASSWORD}@cluster0.tpdoheo.mongodb.net/?retryWrites=true&w=majority")
        mydb = myclient["job"]
        mycol = mydb["jobAd"]
        mycol.insert_many(data_dict)
    except Exception as e:
        print("Error:", str(e))

def runJobsdb():
    print("Start Time", datetime.now())

    #today = datetime.now().date().strftime('%Y-%m-%d')
    headers = {'jobtitle':[],'company':[],'joblocation':[],'jobdesc':[],'jobcate':[],'jobtype':[]}
    df = pd.DataFrame(headers)

    url = "https://hk.jobsdb.com/jobs-in-information-communication-technology"
    my_header = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1"}
    r = requests.get(url, headers= my_header)

    soup = BeautifulSoup(r.text, 'lxml')
    detail_div = soup.find_all("article")

    #find out the number of total jobs
    target_div = soup.find("span", {"data-automation":"totalJobsCount"})

    totalJobs = int(target_div.text.replace(",", ""))

    #identify how many rounds to loop
    rounds = math.ceil(totalJobs/32)

    #parse html
    def parseHTML(detail_div):
        jobinfo = {}
        jobinfo["id"] = f"{today}-p{page}-a{j+1}"

        a = detail_div[j].find(string=True)
        #jobinfo["jobtitle"] = a.text
        jobtitle = detail_div[j].find("a", {"data-automation": "jobTitle"}).text
        jobinfo["jobtitle"] = jobtitle
        print(jobtitle)
        jobdesc = detail_div[j].find_all("li")

        jobinfo["jobdesc"] = reading_info(jobdesc)
        jobcompany = detail_div[j].find("a", {"data-automation": "jobCompany"})
        jobinfo["company"] = reading_info(jobcompany)
        joblocation = detail_div[j].find_all("a", {"data-automation": "jobLocation"})
        jobinfo["joblocation"] = reading_info(joblocation)
        jobcate = detail_div[j].find_all("a", {"data-automation": "jobCardCategoryLink"})
        jobinfo["jobcate"] = reading_info(jobcate)

        jobtype = detail_div[j].find_all("a", {"data-automation": "jobCardJobTypeLink"})
        jobinfo["jobtype"] = reading_info(jobtype)

        '''
        #following script was used before the revamp of jobsdb website in 2024 Jan
        jobinfo["jobtitle"] = a.text
        jobdesc = detail_div[j].find("div", {"data-automation": "job-card-selling-points"}).find_all("span")
        jobinfo["jobdesc"] = reading_info(jobdesc)
        jobcompany = detail_div[j].find("a", {"data-automation": "jobCardCompanyLink"})
        jobinfo["company"] = reading_info(jobcompany)
        joblocation = detail_div[j].find_all("a", {"data-automation": "jobCardLocationLink"})
        jobinfo["joblocation"] = reading_info(joblocation)
        jobcate = detail_div[j].find_all("a", {"data-automation": "jobCardCategoryLink"})
        jobinfo["jobcate"] = reading_info(jobcate)
        jobtype = detail_div[j].find_all("a", {"data-automation": "jobCardJobTypeLink"})
        jobinfo["jobtype"] = reading_info(jobtype)
        '''

        return jobinfo

    #start to scrape
    for page in range(1,rounds+1):
        if page > 1: 
            #updating url request
            #url2 = f"https://hk.jobsdb.com/hk/search-jobs/information-technology-(it)/{page}"
            url2=f"https://hk.jobsdb.com/jobs-in-information-communication-technology?page={page}"
            try:
                r = requests.get(url2, headers=my_header)
                if r.ok:
                    print(r.url)
                    time.sleep(1)
                    soup = BeautifulSoup(r.text, 'lxml')
                    detail_div = soup.find_all("article")
                    
            except:
                print("error")
                continue

        for j in range(len(detail_div)):
            jobinfo = parseHTML(detail_div)
            df = pd.concat([df, pd.DataFrame([jobinfo])], ignore_index=True)

    #remove duplicated articles
    df['company'] = df['company'].fillna('N/A')
    df = df[~df[['jobtitle', 'company']].duplicated() | df['company'].eq('N/A')]

    df['extracted_date'] = today
    df['source'] = 'JobsDB'

    #create a csv file
    df.to_csv(f"jobsdb/jobsdbtest-{today}-n.csv", encoding='utf-8', index=False)
    insertToMongo(df)
    print("Finish Time", datetime.now())

def runCt():
    cate_df = pd.read_excel('ctgoodjob_it_cate_list.xlsx')
    cate_df.set_index('Category', drop=True, inplace=True)
    cate_dict = cate_df.to_dict('index')

    print("Start Time", datetime.now())
    # today = datetime.now().date().strftime('%Y-%m-%d')
    headers = {'jobtitle':[],'company':[],'joblocation':[],'jobdesc':[],'jobcate':[],'jobtype':[]}
    df = pd.DataFrame(headers)

    def parseHTML(article):
        jobinfo = {}
        jobinfo["id"] = f"{today}-i{cateid}p{page}-c{j+1}"
        jobinfo["jobtitle"] = article.select_one('h2>a') and article.select_one('h2>a').text
        jobinfo["company"] = article.select_one('a.jl-comp-name') and article.select_one('a.jl-comp-name').text
        jobinfo["joblocation"] = article.select_one('div.jl-job-info li.loc') and article.select_one('div.jl-job-info li.loc').text.strip()
        highlight = article.select_one('div.jl-job-info ul.job-highlight') and article.select_one('div.jl-job-info ul.job-highlight').find_all('li')
        jobinfo["jobdesc"] = reading_info(highlight)
        jobinfo["jobcate"] = cate

        return jobinfo


    for cate in cate_dict:
        print(f"starting scraping {cate}")
        cateid = cate_dict[cate]["ID"]

        url = f"https://www.ctgoodjobs.hk/ctjob/listing/joblist.asp?job_area={cateid}&page=1"
        my_header = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1"}
        r = requests.get(url, headers=my_header)
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, 'lxml')

        jobarticles = soup.find('div', {'class': ['joblist', 'content']}).find_all('div', {'class': ['row', 'jl-row', 'jl-de']})
        lastpage = soup.find('li', {'class':'last'}) and soup.find('li', {'class':'last'}).find('a')
        if lastpage:
            lastpageno = int(lastpage['href'].split("page=")[-1])
        else:
            lastpageno = 1

        for page in range(1,lastpageno+1):
            if page > 1:
                url2 = f"https://www.ctgoodjobs.hk/ctjob/listing/joblist.asp?job_area={cateid}&page={page}"
                time.sleep(1)
                try:
                    r = requests.get(url2, headers=my_header)
                    r.encoding = 'utf-8'
                    if r.ok:
                        soup = BeautifulSoup(r.text, 'lxml')
                        jobarticles = soup.find('div', {'class': ['joblist', 'content']}).find_all('div', {'class': ['row', 'jl-row', 'jl-de']})
                except:
                    print("error")
                    continue
            #extract and parse
            for j,article in enumerate(jobarticles):
                jobinfo = parseHTML(article)
                if jobinfo["jobtitle"]:
                    df = pd.concat([df, pd.DataFrame([jobinfo])], ignore_index=True)

    df['extracted_date'] = today
    df['source'] = 'ctgoodjobs'

    #group by and concat the category string
    df['jobcate'] = df.groupby(['jobtitle', 'company'])['jobcate'].transform(lambda x: ','.join(x.drop_duplicates()))
    df = df.drop_duplicates(['jobtitle', 'company'])

    #create a csv file
    df.to_csv(f"ct/ctgoodjobs-{today}-n.csv", encoding='utf-8', index=False)
    #insert to sql
    insertToMongo(df)

    print("Finish Time", datetime.now())


runJobsdb()

runCt()