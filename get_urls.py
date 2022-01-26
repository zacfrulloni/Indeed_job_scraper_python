from re import L
import boto3
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pprint
import random
import re
from multiprocessing import Pool

dynamodb = boto3.resource('dynamodb',region_name='eu-west-2',aws_access_key_id='',aws_secret_access_key='')
table = dynamodb.Table('skills_in_demand')

def get_urls(next_page, page):
    """scrape data from initial url and pass urls to query_urls function"""
    print(f'you are on page: {page}')
    open('page_source.html', 'w').close()
    url = f'https://uk.indeed.com/jobs?q=cloud+engineer&limit=50&radius=25&start={next_page}'
    res1 = requests.get(url)
    # GET HTML OF CURRENT PAGE
    our_file = open("page_source.html", "w")
    our_file.write(res1.text)

    # GET URLS:
    list_urls = []
    with open('page_source.html', 'r') as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        for first in soup.find_all("div", attrs={'class': 'mosaic-zone'}):
            for second in first.find_all("div", attrs={'id': 'mosaic-provider-jobcards'}):
                for urls in second.find_all("a", attrs={'data-hiring-event': 'false'}):
                    list_urls.append(urls['href'])
    
    # SCRAPE PAGE_SOURCE.HTML:
    # DATE
    date_posted_list = []
    with open('page_source.html', 'r') as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        dates=soup.find_all("span", attrs={'class': 'date'})
        for date in dates:
            date1 = date.text
            date2 = date1.replace('Posted', '')
            date_posted_list.append(date2)

    # ADD TODAYS DATE
    date_pulled = []    
    today = datetime.today().strftime('%Y-%m-%d')
    for i in date_posted_list:
        date_pulled.append(today)

    # SALARY
    salary_list = []
    with open('page_source.html', 'r') as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        for salary in soup.find_all("div", attrs={'class': 'salary-snippet'}):
            salary_list.append(salary.text)

    # SOME CARDS DONT HAVE A SALARY --> JUST ADD EMPTY UNTIL LIST HAS 50 ITEMS
    while len(salary_list) < 50:
        salary_list.append('empty')

    # LOCATION
    location_list = []
    with open('page_source.html', 'r') as f:
            contents = f.read()
            soup = BeautifulSoup(contents, 'lxml')
            for location in soup.find_all("div", attrs={'class': 'companyLocation'}):
                location_list.append(location.text)

    # JOB_TITLE
    job_title_list = []
    with open('page_source.html', 'r') as f:
            contents = f.read()
            soup = BeautifulSoup(contents, 'lxml')
            for jobs in soup.find_all("h2", attrs={'class': 'jobTitle'}):
                job_title = jobs.text
                job_title = job_title.replace('new', '')
                job_title_list.append(job_title)

    # APPEND FIRST PART OF URL + ASSERT IT'S NOT ALREADY IN URL:
    first_part = 'https://uk.indeed.com'
    new_url_list = []
    for url in list_urls:
        if first_part not in url:
            new_url = first_part + url
            new_url_list.append(new_url)
    try:
        for a, b, c, d, e, f in zip(new_url_list, date_posted_list, job_title_list, salary_list, location_list, date_pulled):
            table.put_item(
                Item={
                    'JOB_URL': a,
                    'DATE_POSTED': b,
                    'JOB_TITLE': c,
                    'SALARY': d,
                    'JOB_LOCATION': e,
                    'DATE_CREATED': f
                        })
    except Exception as error:
        print(error)
    our_file.close()

    return new_url_list

def query_urls(urls):
    """query urls returned from get_urls function and get skills"""
     # SEARCH FOR SKILLS
    skills_list = ['azure', 'aws', 'python', 'javascript', 'linux', 'html5', 'css', 'php', 'sql', 'c\+\+', 'ruby', '.net', 
    'windows', 'macos', 'android', 'ios', 'google cloud', 'amazon web', 'kamatera', 'oracle', 'cisa', 'olap', 'github', 
    'react.js', 'angular', 'java', 'aritificial intelligence', 'machine learning', 'ml', 'deep learning', 
    'batch processing', 'step function', 'rest', 'lan', 'scrum', 'ec2', 'ebs', 'elb', 's3', 'vpc', 'glacier', 'iam', 
    'cloudwatch', 'kms', 'shell', 'unix', 'ecs', 'kafka', 'gcp', 'vpn', 'eks', 'aurora rds', 'lambda', 'kotlin', 'auth0', 
    'api gateway', 'node', 'django', 'iot', 'jenkins', 'iac', 'git', 'ansible', 'ci/cd', 'cli', 'ubuntu', 'centos', 'automation',
     'backup & recovery', 'disaster recovery', 'middleware', 'nosql', 'nosql databases', 'databases', 'networking', 
     'content delivery network', 'data analysis', 'data architecture', 'data mining', 'relational databases', 'data processing', 
     'data quality', 'devops', 'service desk', 'software testing', 'etl', 'dynamodb', 'cloudfront', 'rds', 'ui design', 'user interface',
      'big data', 'terraform', 'docker', 'kubernetes', 'quality assurance', 'serverless architecture', 'saas', 'iaas', 'comptia', 'api', ]

    # user agent
    headers_list = [{
    'authority': 'uk.indeed.com',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,it;q=0.7',
    'cookie': 'CTK=1fm3hi88qt7rd800; CSRF=gHcNKETbCvNZhzzepFNEDdRj0hrgH8xe; INDEED_CSRF_TOKEN=EFHxeADPDqxHlbe09qO14TfQbKVefnuf; CTK=1fm3hi88qt7rd800; SURF=a7OBPW45a4Has2t8hAVHGoTUn41N2EQP; pjps=1; MICRO_CONTENT_CSRF_TOKEN=0ypK0304NQO7Z2HVyJG5Xb2xgVynYHOo; PPID=""; SHARED_INDEED_CSRF_TOKEN=EFHxeADPDqxHlbe09qO14TfQbKVefnuf; indeed_rcc="PREF:LV:CTK:UD:RQ"; OptanonAlertBoxClosed=2021-12-05T22:29:18.891Z; _gcl_au=1.1.180596737.1638743359; _ga=GA1.2.1839982298.1638743359; PREF="TM=1638830437534:L="; ROJC=9d3b2365cc92bbbf:55f0828f367b64e3; RF="oEgFfmQanruHgiqmnlMwApkQSEGmFs_OxrK1R4_e3UQAsmIeU0odYARyKu5yWJPfztE0M5DjWuEp-h-ZTJicIA=="; _mkto_trk=id:699-SXJ-715&token:_mch-indeed.com-1640295912741-34411; RSJC=54930b386d3ea93f:000897e6eb3aff71:55c9d69784390405:585e9bb67c12c824:b9067b99e400bbe2:964108b65648bafe:48b223b81411ef47:575cf7e5806afc91:9ac372ff5aee0fd4; loctip=1; RQ="q=cloud+engineer&l=&ts=1641476760712&pts=1641407977179:q=Senior+Technical+Support+Engineer%2C+SaaS+%28remote%29%09&l=&ts=1639169654495:q=cloud+engineer&l=eh482fn&ts=1638830428344:q=&l=eh482fn&limit=50&ts=1638830386686&pts=1638744721787:q=%22cloud+engineer%22&l=&sort=date&limit=50&ts=1638744702430:q=%22cloud+engineer%22&l=london&limit=50&radius=100&ts=1638744651829"; jaSerpCount=42; LV="LA=1641476760:LV=1638829965:CV=1641476734:TS=1638649110"; UD="LA=1641476760:LV=1638829965:CV=1641476734:TS=1638649110"; OptanonConsent=isIABGlobal=false&datestamp=Sat+Jan+08+2022+18%3A27%3A39+GMT%2B0000+(Greenwich+Mean+Time)&version=6.13.0&hosts=&consentId=b855e5f8-5af6-489e-afa5-83098fc835a0&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B; ac=qTb5MHCwEeyPN7t7ooJ06w#qTduYHCwEeyPN7t7ooJ06w; _gid=GA1.2.269785123.1641666460; _gat=1; LC="co=GB"; JSESSIONID=A1C218B5FBA68B4F8541F1C50F789D08',
    }]

    # proxies
    proxies_list = ['187.73.68.14', '138.204.68.42', '34.124.138.205', '202.154.180.53', '58.187.46.247', '195.170.38.230', '93.157.163.66', '5.131.243.252', '134.119.206.110', '169.57.1.85', '43.129.171.179', '129.159.133.74', '133.167.121.133', '116.202.22.192']
    # while True:
    try:
        headers = random.choice(headers_list)
        proxies = { 'http': random.choice(proxies_list) }
        time.sleep(random.randint(2,3))
        res = requests.get(urls, headers=headers, proxies=proxies)
        time.sleep(random.randint(2,3))
        res=res.text
        # GET SKILLS
        soup=BeautifulSoup(res, 'lxml')
        jobdescription = soup.find("div", id="jobDescriptionText")
        jobdescription = jobdescription.text

        # ASSERT OUR LIST CONTAINS SKILLS THAT ARE LOWER CASE:
        for skill in skills_list:
            assert(skill.islower())

        new_list = []
        for skill in skills_list:
            if re.search(skill, jobdescription):
                new_list.append(skill)
        skills_string = ' '.join(new_list)
        # print(skills_string)
        return skills_string
    except Exception as e:
        print(e)
    
def update_db(skills, urls):
    """update table with skills"""
    try:
        for a, b in zip(urls, skills):
            table.update_item(
                Key={'JOB_URL': a},
                UpdateExpression = "set SKILLS = :s",
                ExpressionAttributeValues={
                    ':s': b
                })
        print('updated')
    except Exception as error:
        print(error)

def main(next_page, page):
    urls = get_urls(next_page, page)
    p = Pool(processes=5) # create pool of workers
    time.sleep(random.randint(2,3)) # wait
    list_of_skills = p.map(query_urls, urls) # call function
    p.terminate() # terminate the processes
    p.join() # wait for worker processes to terminate
    count= 0
    for x in list_of_skills:
        count = count + 1
    print(f'iterations {count}')
    print(list_of_skills)
    update_db(list_of_skills, urls)
    next_page = next_page + 50
    page = page + 1
    main(next_page, page)
if __name__ == "__main__":
    main(next_page = 0, page = 1)