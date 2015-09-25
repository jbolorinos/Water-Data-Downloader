library('ggplot2')
library('reshape2')
library('plyr')
library('dplyr')
library('stringr')
library('data.table')
library('gridExtra')
library('scales')
library('lazyeval')
library('labeling')
library('gtools')
library('mvmeta')
options(stringsAsFactors = TRUE)

input_path = "/Users/user/Documents/California Water Data/Groundwater Level Data"
data_output_path = "/Users/user/Documents/California Water Data/Groundwater Level Data/R-output"
sumstats_output_path = "/Users/user/Documents/California Water Data/Groundwater Level Data/R-output/Sumstats"
graphs_output_path = "/Users/user/Documents/California Water Data/Groundwater Level Data/R-output/Graphs"

setwd(input_path)
regions = c('Central_Coast', 'Colorado_River', 'North_Coast', 'North_Lahontan', 'Sacramento_River', 'San_Francisco_Bay', 'San_Joaquin_River', 'South_Coast', 'South_Lahontan', 'Tulare_Lake')
region_names = gsub(regions, pattern = '_', replacement = ' ')
file_list = paste0(regions,'_gwl_well_data.csv')

# Create single well data file from all of the separate regional well data files
all_well_data = rbindlist(lapply(file_list,fread, na.strings = "NA"))
all_well_data = mutate(all_well_data,
                       Measurement_Date = as.Date(Measurement_Date,"%m-%d-%Y"),                       
                       Region = as.factor(Region),
                       Basin = as.factor(Basin),
                       Use = as.factor(Use),
                       measurement_year = year(Measurement_Date))

get_na_grid = function(start_year, end_year, mode = "All"){
   
   # Set average age of well assumed when it is first observed
   av_age_at_first_obs = 5
   
   # Get wells and years for which there are water level data 
   wells_nonmiss_uyr = mutate(
      distinct(
         filter(all_well_data, !is.na(RPWS) & !is.na(GSWS)), 
         State_Well_Number, measurement_year
         ),
      data_status = 'non_missing'
   )

   # Use this to create an index of wells and years
   wells_nonmiss_all_yrs = rbindlist(
      lapply(
         c(start_year:end_year), 
         function(year){
            mutate(distinct(select(wells_nonmiss_uyr, Region, Basin, Township, State_Well_Number)), measurement_year = year)
         }
      )
   ) 

   # Merge this index with unique-by-year well data to get data for yearly missingness   
   well_dat_na_grid = merge(filter(wells_nonmiss_uyr, measurement_year >= start_year & measurement_year <= end_year), 
                            wells_nonmiss_all_yrs, 
                            by = c('Region', 'Basin', 'Township', 'State_Well_Number', 'measurement_year'),
                            all = TRUE)   
   well_dat_na_grid = mutate(well_dat_na_grid, 
                             data_status = ifelse(is.na(data_status),'missing', data_status), 
                             data_status = as.factor(data_status)
   )      
   
   # If specified, get data for yearly missingness that counts a well as missing only after it has been observed
   if(tolower(mode) == 'first_nonmiss'){
      wells_first_obs = summarize(group_by(wells_nonmiss_uyr, State_Well_Number),
                                  first_obs_year = min(measurement_year))
      well_dat_na_grid = filter(
         mutate(
            merge(well_dat_na_grid, wells_first_obs, by = 'State_Well_Number', all = TRUE),
            well_age = ifelse(!is.na(first_obs_year), measurement_year - first_obs_year + av_age_at_first_obs, NA)
         ),
         measurement_year >= first_obs_year
      )
         
   }
   
   # Get a similar index for townships, basins, and wells for which we know no data are available
   # combine it with the 'well_dat_na_grid' dataset
   missing_data = distinct(filter(all_well_data,is.na(Use)), Region, Basin, Township)  
   missing_data_all_yrs = rbindlist(
      lapply(
         c(start_year:end_year),
         function(year){
            mutate(missing_data, data_status = 'missing', measurement_year = year)
         }
      )
   )
   well_dat_na_grid = rbindlist(list(well_dat_na_grid, missing_data_all_yrs), fill = TRUE)   
   
   return(well_dat_na_grid)
}

well_dat_na_grid = get_na_grid(1950, 2010, 'first_nonmiss')

# Get summary statistics
get_sumstats = function(geo_units){

   if(length(geo_units) == 1){
      well_dat_na_grid = mutate(well_dat_na_grid, geo_unit = Region)
      all_well_data = mutate(all_well_data, geo_unit = Region)
   }
   if(length(geo_units) == 2){
      well_dat_na_grid = mutate(well_dat_na_grid, geo_unit = paste(Region, Basin, sep = '_'))
      all_well_data = mutate(all_well_data, geo_unit = paste(Region, Basin, sep = '_'))
   }
   if(length(geo_units) == 3){
      well_dat_na_grid = mutate(well_dat_na_grid, geo_unit = paste(Region, Basin, Township, sep = '_'))
      all_well_data = mutate(all_well_data, geo_unit = paste(Region, Basin, Township, sep = '_'))
   }
   
   nonmiss_counts = summarize(group_by(well_dat_na_grid, geo_unit, measurement_year, data_status), n_data_status = n()) 
   nonmiss_counts_wide = mutate(
      dcast(as.data.frame(nonmiss_counts),
            geo_unit + measurement_year ~ data_status,
            value.var = "n_data_status"
      ),
      missing = ifelse(is.na(missing), 0, missing),
      non_missing = ifelse(is.na(non_missing), 0, non_missing),
      n_known_wells = non_missing + missing,
      n_observed = non_missing,
      mean_nonmissing = n_observed/n_known_wells
   )   
   geo_sep = colsplit(nonmiss_counts_wide$geo_unit, '_', names = geo_units)
   na_sumstats = cbind(geo_sep, select(nonmiss_counts_wide, measurement_year, n_known_wells, n_observed, mean_nonmissing))
   
   # Get summary stats for the proxy for the water level (RPWS) in each region
   yrly_rpws = as.data.frame(
      summarize(
         group_by(all_well_data, geo_unit, measurement_year),
         n_observed = n(), 
         median_level = median(RPWS, na.rm = TRUE),
         mean_level = mean(RPWS, na.rm = TRUE)
      )
   )
   geo_sep = colsplit(yrly_rpws$geo_unit, '_', names = geo_units)
   rpws_sumstats = cbind(geo_sep, select(yrly_rpws, measurement_year, n_observed, median_level, mean_level))

   sumstats_list = list(na_sumstats, rpws_sumstats)
   names(sumstats_list) = c('na_sumstats', 'rpws_sumstats')
   return(sumstats_list)
}

sumstats = get_sumstats(c('Region'))

# Start well simulation with a function that gives the likelihood of failure of a well over its lifetime
# Uses inverse logit with a given coefficient on time, and an intercept givin the likelihood of failure at t = 0
failure_function = function(t, coef = 0.04, intercept = -6.9){
   return(inv.logit(coef*(t) + intercept))
}
failure_function(100)

# Get estimate for the well failure rate using an estimate of the actual age distribution 
# of the existing wells in the given start_year
get_1st_yr_fail_rate = function(t, region){
   well_dat_first_year = mutate(well_dat_first_year, well_age = well_age + t)
   first_year_freqs = as.data.frame(ftable(well_dat_first_year$well_age))   
   first_year_freqs = mutate(
      cbind(
         first_year_freqs, 
         data.frame('failure_rate' = as.numeric(lapply(as.numeric(levels(first_year_freqs$Var1)),failure_function)))
      ),
      weight = Freq/sum(first_year_freqs$Freq),
      product = failure_rate*weight
   )
   first_year_freqs
   return(sum(first_year_freqs$product))
}

# Get an estimate for the total number of wells in a region
get_n_wells_region = function(region, year, n_wells_tot){
   na_sumstats_wide = dcast(filter(sumstats$na_sumstats),
                            Region ~ measurement_year,
                            value.var = 'n_known_wells'
   )
   na_sumstats_wide = mutate(na_sumstats_wide, 
                             end_year = na_sumstats_wide[,as.character(year)],
                             n_wells_region = round(end_year/sum(end_year)*n_wells_tot)
   )
   n_wells_region = na_sumstats_wide[na_sumstats_wide$Region == region,dim(na_sumstats_wide)[2]]
   return(n_wells_region)
}

# Get a function that gets well failures and replacements for wells constructed in a given year, 
# or of a group of historical wells, over any range of years
get_well_fail_replace = function(region,
                                 construction_year,
                                 start_year, 
                                 end_year, 
                                 replacement_rate,
                                 failure_function_type,
                                 n_wells_start){   
   
   if (failure_function_type == 'new'){
      used_failure_function = function(year, region){return(failure_function(year - construction_year))} 
   }
   else if (failure_function_type == 'historical'){
      # Get an estimate of the cumulative number of failed wells using the current proportion of unused wells
      # from current data (ie for the year 2010) for the entire state
      unused_well_types = c('Well Use:Unused ', 'Well Use:Destroyed ', 'Well Use:Unused Domestic ')
      # Remove Undetermined or missing well use types to ensure that we are looking at wells whose use is known
      well_dat_current = filter(well_dat_na_grid, measurement_year == 2010, !is.na(Use) & Use != 'Well Use:Undetermined ')
      well_dat_use_freqs = summarize(group_by(well_dat_current, Use), number = n(), proportion = n()/dim(well_dat_current)[1])
      well_dat_use_freqs = mutate(well_dat_use_freqs, unused_type = ifelse(Use %in% unused_well_types, proportion, 0))
      proportion_unused = sum(well_dat_use_freqs$unused_type)
      # Get a dataset of all wells that had been observed up until the first year being modeled for use by the 
      # 'get_1st_yr_fail_rate' function
      well_dat_first_year = filter(well_dat_na_grid, measurement_year == start_year & Region == region)
      used_failure_function = function(year, region){
         return(get_1st_yr_fail_rate(year - start_year, region))
      }
   }
   
   get_row = function(year){
      row = data.frame(
         'year' = year,
         'n_active' = row$n_active - row$n_failures,
         'failure_rate' = used_failure_function(year, region),
         'n_failures' = round(row$n_active*row$failure_rate),
         'n_replaced' = round(row$cum_n_failures*replacement_rate),
         'cum_n_failures' = round(row$cum_n_failures - row$cum_n_failures*replacement_rate + row$n_failures)
      )
      return(row)
   } 

   all_years = data.frame() 
   for(year in c(start_year:end_year)){
      if(failure_function_type == 'new'){
         if (year == construction_year){
            row = data.frame(
               'year' = year, 
               'n_active' = n_wells_start,
               'failure_rate' = used_failure_function(year, region),
               'n_failures' = 0,
               'n_replaced' = 0,
               'cum_n_failures' = 0
            )          
         } else if(year < construction_year){
            row = data.frame(
               'year' = year, 
               'n_active' = NA,
               'failure_rate' = NA,
               'n_failures' = NA,
               'n_replaced' = NA,
               'cum_n_failures' = NA
            )        
         } else{
            row = get_row(year)
         }
      } else{
         if (year == start_year){
            row = data.frame(
               'year' = year,
               'n_active' = round(n_wells_start),
               'failure_rate' = used_failure_function(year, region),
               'n_failures' = round(used_failure_function(year - 1, region)*n_wells_start/(1+growth_rate)),
               'n_replaced' = round(n_wells_start/(1 + growth_rate)*proportion_unused*replacement_rate),
               'cum_n_failures' = 
                  round(
                     (n_wells_start*proportion_unused
                      - n_wells_start*proportion_unused*replacement_rate
                      + used_failure_function(year - 1, region)*n_wells_start
                      )
                     /(1 + growth_rate)
                  )
            )
         }
         else{row = get_row(year)}
      }
      all_years = rbind(all_years, row)
   }
   colnames(all_years) = paste(colnames(all_years), construction_year, sep = '/')
   return(all_years)
}

#Use the 'get_well_fail_replace' function to simulate well growth, failure, and replacement for a given time frame
well_simulation = function(regions, start_year, end_year, replacement_rate, growth_rate, n_wells_tot){
   nyears_ago = 2010 - start_year
   simulated_data = rbindlist(
      lapply(regions,function(region){
         n_wells_start = get_n_wells_region(region, 2010, n_wells_tot)/(1 + growth_rate)^nyears_ago
         for(year in c(start_year:end_year)){
            if(year == start_year){
               output_df = get_well_fail_replace(region, year, start_year, end_year, replacement_rate, 'historical', n_wells_start)[,-1]
               n_new_wells = n_wells_start*growth_rate + output_df[year - start_year + 1,paste('n_failures', year, sep = '/')]
               n_new_wells = round(n_new_wells)
            } else{
              output_df = cbind(output_df, get_well_fail_replace(region, year, start_year, end_year, replacement_rate, 'new', n_new_wells)[,-1])
              n_wells_active_current = n_wells_start*(1 + growth_rate)^(year - start_year)
              n_new_wells = n_wells_active_current*growth_rate
              # Add the n_failures in the current year of wells constructed in every prior year 
              # (starting from the first) to get the new wells necessary to attain the 'growth_rate' given
              for(pyear in c(start_year:year)){
                 n_new_wells = n_new_wells + output_df[year - start_year + 1,paste('n_failures', pyear, sep = '/')]
              }
              n_new_wells = round(n_new_wells)
            }
         }
         output_df = cbind(data.frame('year' = c(start_year:end_year)), output_df)
         # Sum or average all of the _[construction_year] variables so we have a single variable for each year
         output_df_long = melt(output_df, id.vars = c('year'))
         output_df_long = cbind('year' = output_df_long[,'year'], 
                                colsplit(output_df_long$variable, '/', names = c('variable', 'construction_year')),
                                'value' = output_df_long[,'value']
                                )
         output_df_long_uyr = summarize(group_by(output_df_long, year, variable), sum = sum(value, na.rm = TRUE))
         output_df_uyr = dcast(output_df_long_uyr, year ~ variable, value.var = 'sum')
         output_df_uyr = mutate(output_df_uyr[,-3],                              
                                growth_rate = n_active/lag(n_active) - 1,
                                n_new_wells = n_active - lag(n_active) - lag(n_replaced),
                                percent_inactive = cum_n_failures/n_active
                                ) 
         output_df_uyr = cbind(data.frame('Region' = region), output_df_uyr)
         return(output_df_uyr)
      })
   ) 
   return(simulated_data)
}

well_simulation_data = well_simulation('Tulare Lake', 1950, 2100, 0.05, 0.005, 2000000)
View(well_simulation_data, title = 'well_simulation_data')

well_simulation_sumstats = summarize(group_by(well_simulation_data, year), n_new_wells = sum(n_new_wells), n_wells = sum(n_active))
View(well_simulation_sumstats, title = 'well_simulation_sumstats')

# Plots!
yearly_freqs_plot = ggplot(regions_sumstats, aes(measurement_year, mean_nonmissing, colour = Region))+
   geom_point(aes(size = n_wells))+
   geom_line(aes(colour = Region))+
   scale_x_continuous(limits = c(1950,2010), breaks = c(1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010))
yearly_freqs_plot

