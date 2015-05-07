from urllib import request, parse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import sqlite3
import queue
import shutil
import csv
import os
from os import path
import time


def get_form_field_names(url): 
    import urllib.request, urllib.parse
    page_content = urllib.request.urlopen(url).read()
    forms = BeautifulSoup(page_content).findAll('form')
    form_list = []
    for form in forms:
        if form.has_attr('id'):
            form_list.append(form['id'])
    return form_list
 

def download_all_water_data(data_format, browser, wql_start_url, region_list, download_dir, download_filename, wait_time_interval):
    start_time = time.clock()
    default_download_dir = '/Users/user/Downloads'
    default_download_filename = 'GWLData.csv'
    
    if browser == 'Chrome':
        driver = webdriver.Chrome()
    elif browser == 'Firefox':
        driver = webdriver.Firefox()
    elif browser == 'Ie':
        driver = webdriver.Ie()
    else:
        driver = webdriver.Remote()
    
    # Open the water quality download index page    
    driver.get(wql_start_url)

    # Start a loop through all of the regions
    for region in region_list:
        # Reload the URL for each iteration so objects aren't lost
        driver.get(wql_start_url)
        region_selection = Select(driver.find_element_by_name('select1'))
        print("Checking Region: " + region)
        region_selection.select_by_visible_text(region)
        driver.find_element_by_name('Search1').click()
        all_basins = driver.find_element_by_xpath("//select[@name = 'select2']")
        all_basins = all_basins.find_elements_by_tag_name('option')
        all_basin_names = []
        
        wells_with_data = []
        
        for basin in all_basins:
            all_basin_names.append(basin.get_attribute('text'))
        if all_basin_names != ['']:
            for basin in all_basin_names:
                # Reload the URL and data for each iteration so objects aren't lost
                driver.get(wql_start_url)
                if basin != "":
                    print('Checking Basin: ' + basin)
                    # Reload the URL and data for each iteration so objects aren't lost
                    region_selection = Select(driver.find_element_by_name('select1'))
                    region_selection.select_by_visible_text(region)
                    driver.find_element_by_name('Search1').click() 
                    
                    # Proceed to selection of basin in loop
                    basin_selection = Select(driver.find_element_by_name('select2'))                   
                    basin_selection.select_by_visible_text(basin)
                    driver.find_element_by_name('Search1').click()
                    
                    # Get data for townships (if any)
                    all_townships = driver.find_element_by_xpath("//select[@name = 'select3']")
                    all_townships = all_townships.find_elements_by_tag_name('option')
                    all_township_names = []
                    for township in all_townships:
                        all_township_names.append(township.get_attribute('text'))
                    if all_township_names != ['']:
                        for township in all_township_names:
                            # Reload the URL and data for each iteration so objects aren't lost
                            driver.get(wql_start_url) 
                            if township != "":
                                
                                print('Checking Township: ' + township)
                                
                                # Reload the URL and data for each iteration so objects aren't lost  
                                region_selection = Select(driver.find_element_by_name('select1'))
                                region_selection.select_by_visible_text(region)
                                driver.find_element_by_name('Search1').click()                                     
                                basin_selection = Select(driver.find_element_by_name('select2'))                   
                                basin_selection.select_by_visible_text(basin)
                                driver.find_element_by_name('Search1').click()
                                
                                # Proceed to selection of township in loop                                   
                                township_selection = Select(driver.find_element_by_name('select3'))
                                township_selection.select_by_visible_text(township)
                                driver.find_element_by_name("cmdNext").click()
                                
                                # Get data for wells (if any) and download it!
                                all_wells = driver.find_element_by_xpath("//select[@name = 'wellNumber']")
                                all_wells = all_wells.find_elements_by_tag_name('option')
                                all_well_names = []
                                for well in all_wells:
                                    all_well_names.append(well.get_attribute('text'))
                                if all_well_names != []:
                                    for well in all_well_names:
                                        download_filename = region.replace(' ','-') + '_' + basin.replace(' ','-') + '_' + well + '.csv'
                                        select_well = Select(driver.find_element_by_name('wellNumber'))
                                        select_well.select_by_visible_text(well)                                            
                                        select_format = Select(driver.find_element_by_name('OPformat'))
                                        select_format.select_by_visible_text(data_format)
                                        
                                        driver.find_element_by_name('cmdSubmit').click()
                                        print('Downloading data for: ' + download_filename)

                                        
                                        # Wait until the file has finished downloading before moving it and continuing the loop
                                        wait_time = 0
                                        while os.path.isfile(default_download_dir + '/' + default_download_filename) == False:
                                            time.sleep(wait_time_interval)
                                            wait_time += wait_time_interval
                                            if wait_time > 60:
                                                print('Error: Script Aborted because file for: ' + download_filename + ' never finished downloading')
                                                return                                             
                                        
                                        # Move file to the directory where you want it and name it appropriately
                                        os.rename(default_download_dir + '/' + default_download_filename, download_dir + '/' + download_filename)                                        
                                        # Wait until the file has finished being moved before continuing the loop
                                        wait_time = 0
                                        while os.path.isfile(download_dir + '/' + download_filename) == False:
                                            time.sleep(wait_time_interval)
                                            wait_time += wait_time_interval
                                            if wait_time > 10:
                                                print('Error: Script Aborted because ' + download_dir + '/' + download_filename + ' was not renamed')
                                                return        
                                        # If it it hasn't been done already, delete the source file in the downloads folder
                                        if os.path.isfile(default_download_dir + '/' + default_download_filename):
                                            os.remove(default_download_dir + '/' + default_download_filename)
                                            # Wait until the file has finished being deleted before continuing the loop
                                            wait_time = 0
                                            while os.path.isfile(default_download_dir + '/' + default_download_filename) == True:
                                                time.sleep(wait_time_interval)
                                                wait_time += wait_time_interval
                                                if wait_time > 10:
                                                    print('Error: Script Aborted because ' + default_download_dir + '/' + default_download_filename + ' was not deleted')
                                                    return                                                                                                     
                                                                              
                                        # Print a message to the log and keep a record of all of the wells/townships/basins/regions with data
                                        print('Successfully downloaded ' + download_filename)
                                        if well not in wells_with_data:
                                            wells_with_data.append([region,basin,township,well])
    
    driver.close()
    end_time = time.clock()
    minutes_elapsed = (end_time - start_time)/60
    print('Data finished downloading. The process took: ' + minutes_elapsed + ' minutes')
    return wells_with_data                      

wells_with_data = download_all_water_data('Text', 
                                          'Chrome',
                                          'http://www.water.ca.gov/waterdatalibrary/groundwater/hydrographs/index.cfm',
                                          ['Sacramento River'],
                                          '/Users/user/Documents/California Water Data/Groundwater Level Data',
                                          'GWL_Data',
                                          0.1)
