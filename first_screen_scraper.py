import glob
import os

from dotenv import load_dotenv
import boto3
import time

from datetime import datetime, timedelta
import pandas as pd
import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

s3 = boto3.resource('s3')
s3client = boto3.client('s3', region_name='us-east-2')

options = Options()
# options.headless = True
options.add_argument("--no-sandbox")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
load_dotenv()
driver.get(os.getenv('URL'))
# driver.get('https://nmrldlpi.force.com/bcd/s/public-search-license?division=CCD&language=en_US')
time.sleep(4)

# Clicking on the License Status Radio Button
# WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,
#                                                            '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-radio-group/slot/fieldset/div[1]/div[2]/div[2]/label[4]/span[1]'))).click()


# Clicking on the the License Type Radio Button
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,
                                                            '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-radio-group/slot/fieldset/div[1]/div[2]/div[2]/label[1]/span[1]'))).click()

time.sleep(7)

# Clicking on the Dropdown  - License Type
driver.find_element(By.XPATH,
                    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-select[1]/slot/c-combobox/div/div/div[2]/div[1]/div/input').click()

time.sleep(3)

# Click on the drop down combobox
print('clicked on combobox')
time.sleep(2)
driver.find_element(By.XPATH,
                    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-select[1]/slot/c-combobox/div/div/div[2]/div[2]/div/ul/li[2]/div/span/span').click()
time.sleep(4)

WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,
                                                            '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-block/ul/li/section/div[2]/slot/vlocity_ins-omniscript-ip-action/slot/div/c-button/button'))).click()
time.sleep(20)
total_pages = [sub for sub in driver.find_element(By.XPATH,
                                                  '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-text-block[6]/slot/div/c-omniscript-formatted-rich-text/lightning-formatted-rich-text/span/div/p').text.split()
               if sub.isdigit()][1]
print(total_pages)


def create_list_of_columns(list_of_xpaths, new_column_list):
    for elem in range(0, len(list_of_xpaths)):
        print(elem)
        new_column_list[elem] = []
    return new_column_list


def creating_df(df_name, column_list_data):
    for elem in range(0, len(column_list_data)):
        df_name[elem] = column_list_data[elem]
    return df_name


def insert_data(file_name, data):
    with open(file_name, 'w+') as csvfile:
        writer = csv.writer(csvfile)
        for index, row in data.iterrows():
            writer.writerow(row)


def scrape_pages(list_of_xpath, list_of_columns, df_col_list, page_counter, output_file):
    data_df = pd.DataFrame()
    for page in range(int(page_counter)):
        for elem in range(0, len(list_of_xpath)):
            for col_name in driver.find_elements(By.XPATH, list_of_xpath[elem]):
                list_of_columns[elem].append(col_name.text)

        all_data_df = pd.DataFrame()
        df = creating_df(df_name=all_data_df, column_list_data=list_of_columns)
        df.columns = df_col_list

        print("DataFrame: ", page)
        print(df)
        insert_data(output_file, data=df)
        # Next Button Click
        time.sleep(7)
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH,
                                                                        '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-ip-action[3]/slot/div/c-button/button'))).click()
        except ValueError:
            break
        time.sleep(10)

xpath_list = ['/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[1]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
              '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[2]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
              '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[3]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
              '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[4]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div']
# xpath_list = ['/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[1]/vlocity_ins-output-field',
#             '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[2]/vlocity_ins-output-field',
#             '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[3]/vlocity_ins-output-field',
#             '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[4]/vlocity_ins-output-field',
#             '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[4]/vlocity_ins-output-field']

# recent_xpath_list = [
#     '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[1]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
#     '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[2]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span',
#     '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[3]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
#     '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[3]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-application-search_-child/div/vlocity_ins-flex-card-state[1]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[4]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span']

# old_xpath_list = [
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[1]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[3]/vlocity_ins-output-field/div/div/span',
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[2]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div',
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[4]/vlocity_ins-output-field/div/div/span',
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[5]/vlocity_ins-output-field/div/lightning-formatted-rich-text/span/div'
#    '/html/body/div[3]/div[3]/div/div[2]/div/div/c-cf-mtxlpi_fc_comprehensive-license-search-container/div/vlocity_ins-flex-card-state[2]/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div/vlocity_ins-custom-lwc-wrapper/slot/c-c-c-d-comprehensive-license-search-c-c-d-english/div/article/div[1]/vlocity_ins-omniscript-step/div[3]/slot/vlocity_ins-omniscript-custom-lwc[4]/slot/c-cf-mtxlpi_fc_-c-c-dcomprehensive-license-search_-child/div/vlocity_ins-flex-card-state/div/slot/div/div/vlocity_ins-block/div/div/div/slot/div/div[6]/vlocity_ins-output-field/div/div/span'
# ]

column_list = create_list_of_columns(list_of_xpaths=xpath_list,
                                     new_column_list=['license_type', 'application_status', 'business_name',
                                                      'business_address'])

# def write_s3_file(dest_bucket, destination_file, dataframe):
#     s3 = s3fs.S3FileSystem(anon=False)
#     with s3.open(f"s3://{dest_bucket}/{destination_file}/","w") as output_csv_file:
#          dataframe["year"] = year
#          dataframe["month"] = month
#          dataframe["day"] = day
#          dataframe.to_csv(output_csv_file, index=False)


# First Screen Data - Scraper
dest_bucket = os.getenv("DEST_BUCKET")
date = (datetime.today() - timedelta(0)).strftime('%Y%m%d')
year = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[0]
month = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[1]
day = (datetime.today() - timedelta(0)).strftime('%Y-%m-%d').split('-')[2]
landing_filepath = os.getenv("LANDING_FILE") + date + ".csv"
all_data = scrape_pages(list_of_xpath=xpath_list, list_of_columns=column_list,
                        df_col_list=['license_type', 'application_status', 'business_name', 'business_address'],
                        page_counter=total_pages,
                        output_file='first_screen_data_' + date + '.csv')

dest_bucket = os.getenv("DEST_BUCKET")
destination_file = os.getenv("DEST_FILE_LOCATION") + date + os.getenv("DEST_FILE")
csvfile = []
for f in glob.glob('*.csv'):
    if f.startswith('first_screen_data_'):
        csvfile.append(f)

for datafile in csvfile:
    print(datafile)
    #    s3client.upload_file(datafile, Bucket=dest_bucket, Key=destination_file)
    print('File uploaded on S3.')

driver.close()
print("Done.")
