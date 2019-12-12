from elasticsearch import Elasticsearch

es = Elasticsearch("localhost:9200")


import requests
from bs4 import BeautifulSoup
import http
from bs4 import element
from urllib import request
import pprint


# import the necessary packages
import urllib.request
#opencv-contrib-python==3.4.2.17
#opencv-python==3.4.2.17
import cv2 as cv

import numpy as np


def create_index(es,name):
    if not es.indices.exists(name):
        response = es.indices.create(name)

        if response["acknowledged"]:
            print("to create index is successful : index name = '{}'".format(response["index"]))

def index_data(es,index,data,doc_type):
    es.index(index=index, doc_type=doc_type, body=data)

# METHOD #1: OpenCV, NumPy, and urllib
def url_to_image(url):
    # download the image, convert it to a NumPy array, and then read
    # it into OpenCV format
    resp = urllib.request.urlopen(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv.imdecode(image, cv.IMREAD_COLOR)

    # return the image
    return image

def img_url2hash(url):

    img = url_to_image(url)

    gray= cv.cvtColor(img,cv.COLOR_BGR2GRAY)
    sift = cv.xfeatures2d.SIFT_create()

    # Initiate BRIEF extractor
    brief = cv.xfeatures2d.BriefDescriptorExtractor_create()


    kp = sift.detect(img,None)

    # compute the descriptors with BRIEF
    kp, des = brief.compute(img, kp)


    hash_list = []
    for x in des:
        hash_list.append('{}'.format(x.tobytes().hex()))
    return hash_list


category_map = {
    "TV/영상가전" : 1011010000
}

himark_url = 'http://www.e-himart.co.kr'
category_endpoint = 'http://www.e-himart.co.kr/app/display/showDisplayCategory?dispNo='
page_counting_param = '#pageCount='

def get_category_page():
    pass

# HTTP GET Request
req = requests.get('http://www.e-himart.co.kr/app/display/showDisplayCategory?dispNo=1011010000')

es = Elasticsearch("localhost:9200")

if req.status_code == http.HTTPStatus.OK:
    # HTML 소스 가져오기
    html = req.text
    # BeautifulSoup으로 html소스를 python객체로 변환하기
    # 첫 인자는 html소스코드, 두 번째 인자는 어떤 parser를 이용할지 명시.
    # 이 글에서는 Python 내장 html.parser를 이용했다.
    soup = BeautifulSoup(html, 'html.parser')

    product_list = soup.find("ul", attrs={"id": "productList"})

    for product_item in product_list:
        if type(product_item) == element.Tag:

            if product_item.div:
                if product_item.div.a:

                    # product_detail
                    req = requests.get(himark_url+product_item.div.a['href'])
                    if req.status_code == http.HTTPStatus.OK:
                        html = req.text
                        soup = BeautifulSoup(html, 'html.parser')

                        product_info = {}

                        ## title and promote
                        title_div = soup.find("div", attrs={"class": "prdName"})
                        title = title_div.h2.text.lstrip().rstrip()
                        product_info['title'] = title

                        #promote optional
                        if title_div.div:
                            promote = title_div.div.text.lstrip().rstrip()
                            product_info['promote'] = promote

                        ## 모델 명
                        if soup.find("div", attrs={"id": "divModelName"}):
                            model_name = soup.find("div", attrs={"id": "divModelName"}).text
                            model_name = model_name.lstrip().rstrip()
                            product_info['model_name'] = model_name
                        elif soup.find("span", attrs={"class": "foL"}):
                            model_name = soup.find("span", attrs={"class": "foL"}).text
                            model_name = model_name.lstrip().rstrip()
                            product_info['model_name'] = model_name

                        ## 가격
                        price_area = soup.find("li", attrs={"class": "priceArea"}).find_all("span", attrs={"class": "price"})
                        sale_price = int(price_area[0].text.replace(',',''))
                        product_info['sale_price'] = sale_price
                        advantage_price = int(price_area[1].text.replace(',',''))
                        product_info['advantage_price'] = advantage_price

                        #별점 optional
                        if soup.find("div", attrs={"class": "gmL"}):
                            star_point = float(soup.find("div", attrs={"class": "gmL"}).text)
                            product_info['star_point'] = star_point

                        #image
                        img_link = soup.find("img",attrs={"id": "imgGoodsBigImage"})["src"]
                        product_info['img_link'] = img_link
                        img_hash = img_url2hash(img_link)
                        product_info['img_hashs'] = img_hash


                        pprint.pprint(product_info)


