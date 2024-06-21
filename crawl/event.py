from bs4 import BeautifulSoup
from datetime import datetime
import requests
import json
import os
import re

class Event():
    def __init__(self) -> None:
        self.source = 'detik.com'
    
    def get_api(self, url):
        crawling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        crawling_epoch = int(datetime.now().timestamp())
        domain = url.split('/')[2]
        category = url.split('/')[4].split('?')[0]
        tag = [self.source, domain, 'data_event']
        data_raw = {
            'link': url,
            'source': self.source,
            'domain': domain,
            'tag': tag,
            'crawling_time': crawling_time,
            'crawling_time_epoch': crawling_epoch,
            'category': category,
            'data': []
        }
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        for event_url in soup.select('body > div.main-content.page > div > div > div.column-md-8.column-lg > div > div.column > div.list-content.mgb-24 > article > div > div.media__text > div > div.column.pos-static > h3 > a'):
            data = self.get_detail_event(event_url.get('href'))
            data_raw['data'].append(data)
            
        pagination = soup.select('body > div.main-content.page > div > div > div.column-md-8.column-lg > div > div.column > div.pagination.text-center.mgt-16.mgb-24 > a')
        for next_page in pagination:
            if next_page.text.strip() == "Next":
                self.get_api(next_page.get('href'))
                
        return data_raw

    def start(self, url):
        from connection.s3_conn import S3Conn
        s3 = S3Conn()
        
        print(f'getting {url}')
        res = requests.get(url)
        print(f'success getting {url} \n') if res.status_code == 200 else print(f'failed getting {url} \n')
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        for event_url in soup.select('body > div.main-content.page > div > div > div.column-md-8.column-lg > div > div.column > div.list-content.mgb-24 > article > div > div.media__text > div > div.column.pos-static > h3 > a'):
            data = self.get_detail_event(event_url.get('href'))
            
            crawling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            crawling_epoch = int(datetime.now().timestamp())
            file_name = f'{crawling_epoch}.json'
            
            data_raw = {
                'link': url,
                'source': self.source,
                'domain': self.domain,
                'tag': self.tag,
                'crawling_time': crawling_time,
                'crawling_time_epoch': crawling_epoch,
                'path_data_raw': None,
                'category': self.category,
                'data': data
            }
            
            with open(f'F:/Work/Garapan gweh/make api/data_event/detik/crawl/json/{file_name}', 'w') as f:
                json.dump(data_raw, f)
                
            path_s3 = f's3://ai-pipeline-raw-data/data/data_descriptive/{self.source}/data_event/{self.category}/json/{file_name}'
            s3.upload(rawpath=path_s3, localpath=f'F:/Work/Garapan gweh/make api/data_event/detik/crawl/json/{file_name}')
            data_raw['path_data_raw'] = path_s3
            
            os.remove(f'F:/Work/Garapan gweh/make api/data_event/detik/crawl/json/{file_name}')
            
        pagination = soup.select('body > div.main-content.page > div > div > div.column-md-8.column-lg > div > div.column > div.pagination.text-center.mgt-16.mgb-24 > a')
        for next_page in pagination:
            if next_page.text.strip() == "Next":
                self.start(next_page.get('href'))

    def get_detail_event(self, url):
        print(f'getting {url}')
        res = requests.get(url)
        print(f'success getting {url} \n') if res.status_code == 200 else print(f'failed getting {url} \n')
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        image = soup.select_one('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__headline > div > div.media__image > div.ratiobox.ratiobox--16-9 > img').get('data-src')
        title = soup.select_one('h1.media__title').text.strip()
        status = soup.select_one('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__headline > div > div.media__label.media__label--closed')
        status = status.text.strip() if status else None
        date = soup.select_one('div.media__date')
        if date:
            date = date.text.strip()
            match = re.search(r"(\d{2} \w+ \d{4}) - (\d{2} \w+ \d{4})", date)
            if match:
                tanggal_awal = match.group(1)
                tanggal_akhir = match.group(2)
            
        tag = soup.select_one('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__headline > div > div.media__text > div.mgt-16.mgb-16 > a').text.strip()
        location = soup.select_one('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__headline > div > div.media__text > div.event-detail__headline-bottom').text.strip()
        
        highlight = soup.select_one('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > div:nth-child(1)').text.strip()
        
        data_ticket = []
        for ticket in soup.select('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > div.event-detail__highlight > div.grid-row'):
            ticket_name = ticket.select_one('div:nth-child(1) > div:nth-child(1)').text.strip()
            ticket_price = ticket.select_one('div:nth-child(2) > div:nth-child(2)').text.strip()
            ticket_status = ticket.select_one('div:nth-child(3) > div').text.strip()
            
            data_ticket.append({
                'ticket_name': ticket_name,
                'ticket_price': ticket_price,
                'ticket_status': ticket_status
            })
        
        deskripsi = ''
        descs =  soup.select('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > p')
        
        if descs:
            for desc in descs:
                deskripsi += desc.text.strip()
        else:
            descs = soup.select('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > div > span > div')
            for desc in descs:
                deskripsi += desc.text.strip()

        syarat_ketentuan = []
        
        syarats = soup.select('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > div.accordion.accordion-wrap > div > div.accordion-content > div > ul > li')
        if syarats:
            for syarat in syarats:
                syarat_ketentuan.append(syarat.text.strip())
        else:
            syarats = soup.select('body > div.main-content.main-content--single.page.page--event > div > article > div.event-detail__body > div.grid-row.column-content > div > div.accordion.accordion-wrap > div > div.accordion-content > div > div')
            
            for syarat in syarats:
                syarat_ketentuan.append(syarat.text.strip())
        
        data_raw = {
            'link': url,
            'image': image,
            'title': title,
            'status': status,
            'start_date': tanggal_awal if tanggal_awal else date,
            'end_date': tanggal_akhir,
            'tag': tag,
            'location': location,
            'highlight': highlight,
            'data_ticket': data_ticket,
            'description': deskripsi,
            'terms_conditions': syarat_ketentuan
        }
        
        return data_raw

if __name__ == '__main__':
    categories = [
        "semua",
        "konser-dan-pertunjukan",
        "seminar-dan-talkshow",
        "workshop",
        "kontes",
        "lain-lain",
        "kuis",
        "festival"
    ]
    for ctg in categories:
        url = f'https://event.detik.com/kategori/{ctg}'
        event = Event()
        event.start(url)