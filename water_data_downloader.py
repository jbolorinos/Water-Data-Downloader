'''
Created on Mar 31, 2015
Tries to download all of the water quality data 
from the www.water.ca.gov website
@author: jbolorinos
'''

from urllib import request, parse
from bs4 import BeautifulSoup
from selenium.selenium import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import sqlite3
import os
from queue import Queue
import csv
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

def download_all_water_data(data_format, browser, wql_start_url, region_list, combine_all_regions, download_dir, wait_time_interval):
    start_time = time.time()
    default_download_dir = '/Users/user/Downloads'
    default_download_filename = 'GWLData.csv'
    
    # Open the sql connection and prepare sql tables 
    conn = sqlite3.connect(':memory:', check_same_thread = False)
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE well_data(
        id INTEGER PRIMARY KEY,
        Region TEXT,
        Basin TEXT,
        Township TEXT,
        Use TEXT,        
        State_Well_Number TEXT, 
        Measurement_Date TEXT, 
        RP_Elevation TEXT, 
        GS_Elevation TEXT,
        RPWS TEXT,
        WSE TEXT,  
        GSWS TEXT,    
        QM_Code TEXT,    
        NM_Code TEXT,  
        Agency TEXT,    
        Comment TEXT
        )'''
    )
    
    cursor.execute(
        ''' CREATE TABLE well_coords(
        id INTEGER PRIMARY KEY,
        Region TEXT,
        Basin TEXT,
        Township TEXT,
        Use TEXT,
        State_Well_Number TEXT,           
        Projection TEXT,
        Datum TEXT,    
        Easting TEXT,   
        Northing TEXT,   
        Units TEXT,   
        Zone TEXT     
        )'''
    )    
    
    SQL_import_data = """insert into well_data
                         (Region, Basin, Township, Use, State_Well_Number, Measurement_Date, RP_Elevation, GS_Elevation, RPWS, WSE, GSWS, QM_Code, NM_Code, Agency, Comment)
                         values('%s', '%s', '%s', '%s', :State_Well_Number, :Measurement_Date, :RP_Elevation, :GS_Elevation, :RPWS, :WSE, :GSWS, :QM_Code, :NM_Code, :Agency, :Comment)
                      """
    SQL_import_coords = """insert into well_coords
                           (Region, Basin, Township, Use, State_Well_Number, Projection, Datum, Easting, Northing, Units, Zone)
                           values('%s', '%s', '%s', '%s', '%s', :Projection, :Datum, :Easting, :Northing, :Units, :Zone)
                        """         
    empty_imp_data = """insert into well_data
                        (Region, Basin, Township, Use, State_Well_Number, Measurement_Date, RP_Elevation, GS_Elevation, RPWS, WSE, GSWS, QM_Code, NM_Code, Agency, Comment)
                        values('%s', '%s', '%s', '%s', '%s', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA')
                     """
    
    empty_imp_coords = """insert into well_coords
                          (Region, Basin, Township, Use, State_Well_Number, Projection, Datum, Easting, Northing, Units, Zone)
                          values('%s', '%s', '%s', '%s', '%s', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA')
                       """                       
    
    # Initialize a variable name for the Queue() object
    queue = Queue()
    
    # Set browser type according to user specification
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

    def download_csv_from_website(well):
        select_well = Select(driver.find_element_by_name('wellNumber'))
        select_well.select_by_visible_text(well)                                            
        select_format = Select(driver.find_element_by_name('OPformat'))
        select_format.select_by_visible_text(data_format)
        driver.find_element_by_name('cmdSubmit').click() 
        try:
            alert = driver.switch_to_alert()
            alert.accept()
            print("No data available for well: %s" %(well))
            return 0
        except:         
            return 1

    def get_tables_from_csv(input_path): 
        input = csv.reader(open(input_path, 'rt'))
        # Break Input data into multiple csv_objects, returning a list of lists (each being a row in the table) for each table
        tables = []
        table = []
        for row in input:
            if not row:
                tables.append(table)       
                table = []     
            else:
                table.append(row)
        tables.append(table)
        return tables
    
    def load_csv_to_sql(tables):    
        use = 'NA'
        # If the csv file contains two tables then load each (if non-empty) into the sql database
        if tables and len(tables) == 2:
            if tables[1]:
                use = tables[1][len(tables[1])-1][0]   
                cursor.executemany(SQL_import_coords %(region, basin, township, use, well), tables[1][2:len(tables[1])-2] )             
            if tables[0]:         
                cursor.executemany(SQL_import_data %(region, basin, township, use), tables[0][1:])
        # If the csv file only contains one table then load the table into the correct database by checking its header first
        # Insert missing values into the other table database
        elif tables and tables[0]:
            if  'State Well Number' in tables[0][0]:
                cursor.executemany(SQL_import_data %(region, basin, township, use), tables[0][1:])
                cursor.execute(empty_imp_coords %(region, basin, township, use, well))   
                print("No coordinate data available for well: %s" %(well))                       
            elif 'Well Coordinate Information' in tables[0][0]:
                cursor.executemany(SQL_import_coords %(region, basin, township, use, well), tables[1][2:len(tables[1])-2] )
                cursor.execute(empty_imp_data %(region, basin, township, use, well))
                print("No data available for well: %s" %(well))
        # Insert missing values for wells with no data
        else: 
            cursor.execute(empty_imp_data %(region, basin, township, use, well))
            cursor.execute(empty_imp_coords %(region, basin, township, use, well)) 
            print("No data available for well: %s" %(well))
        conn.commit()         
        return
    
    def download_and_store_well_data(well):        
        data_available = download_csv_from_website(well)
        tables = [[]]
        if data_available:
            # Wait until the file has finished downloading before trying to load it into the sql database
            wait_time = 0
            while os.path.isfile(default_download_dir + '/' + default_download_filename) == False:
                time.sleep(wait_time_interval)
                wait_time += wait_time_interval
                if wait_time > 60:
                    print('Error: Script Aborted because the file for well ' + well + ' never finished downloading')
                    quit()         
            tables = get_tables_from_csv(default_download_dir + '/' + default_download_filename)
        try:
            load_csv_to_sql(tables)
        except IndexError:
            print('Skipped data for well %s in %s Region, %s Basin , Township %s because of data irregularity' %(well, region, basin, township))
        except:
            print('Skipped data for well %s in %s Region, %s Basin , Township %s due to unknown error' %(well, region, basin, township))
        if data_available:
            os.remove(default_download_dir + '/' + default_download_filename)        
        print('Downloaded and stored data for %s %s %s %s' %(region, basin, township, well))
        return

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
                                        if well != None:
                                            queue.put(well)
                                    while not queue.empty():
                                        if os.path.isfile(default_download_dir + '/' + default_download_filename) == False:
                                            well = queue.get()
                                            download_and_store_well_data(well)
                                else:
                                    print("No data available for township: %s" %(township))
                                    cursor.execute(empty_imp_data %(region, basin, township, 'NA', 'NA'))
                    else:
                        print("No data available for %s basin" %(basin))
                        cursor.execute(empty_imp_data %(region, basin, 'NA', 'NA', 'NA'))                
        else:
            print("No data available for %s region" %(region))
            cursor.execute(empty_imp_data %(region, 'NA', 'NA', 'NA', 'NA'))
            
        #Write the region's well data in memory to a csv file in the desired directory
        all_rows = cursor.execute("""SELECT * FROM well_data WHERE Region = '%s' """ %(region))        
        with open(download_dir + '/%s_gwl_well_data.csv' %(region.replace(' ','-')), 'w', newline = '' ) as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            # Write the header to the output file
            writer.writerow([i[0] for i in all_rows.description])          
            for row in all_rows:
                writer.writerow(row)
                
        #Write the region's well coordinate information in memory to a csv file in the desired directory
        all_rows = cursor.execute("""SELECT * FROM well_coords WHERE Region = '%s' """ %(region))        
        with open(download_dir + '/%s_well_coordinate_data.csv' %(region.replace(' ','-')), 'w', newline = '' ) as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            # Write the header to the output file
            writer.writerow([i[0] for i in all_rows.description])          
            for row in all_rows:
                writer.writerow(row)
        
        minutes_elapsed = (time.time() - start_time)/60
        print('Finished downloading well data for %s. The process took %i minutes' %(region, minutes_elapsed))
                
    # Close browser when Finished
    driver.close()
    
    if combine_all_regions:
        #Write the well data in memory to a csv file in the desired directory
        all_rows = cursor.execute('''SELECT * FROM well_data''')        
        with open(download_dir + '/all_regions_gwl_well_data.csv', 'w', newline = '' ) as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            # Write the header to the output file
            writer.writerow([i[0] for i in all_rows.description])
            for row in all_rows:
                writer.writerow(row)
                
        #Write the well coordinate information in memory to a csv file in the desired directory
        all_rows = cursor.execute('''SELECT * FROM well_coords''')        
        with open(download_dir + '/all_regions_well_coordinate_data.csv', 'w', newline = '' ) as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            # Write the header to the output file
            writer.writerow([i[0] for i in all_rows.description])
            for row in all_rows:       
                writer.writerow(row)
                
    #Close SQL connection
    conn.close()       
    
    #Print time elapsed to the log
    minutes_elapsed = (time.time() - start_time)/60
    print('Data finished downloading. The process took: ' + minutes_elapsed + ' minutes')                      

download_all_water_data('Text', 
                        'Chrome',
                        'http://www.water.ca.gov/waterdatalibrary/groundwater/hydrographs/index.cfm',
                        ['Central Coast', 'Colorado River', 'North Coast', 'North Lahontan', 'Sacramento River', 
                         'San Francisco Bay', 'San Joaquin River', 'South Coast' , 'South Lahontan', 'Tulare Lake'],
                        1,
                        '/Users/user/Documents/California Water Data/Groundwater Level Data',
                        0.1
                        )
