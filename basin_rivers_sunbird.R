# 23/05/2019
# Josh Jones
# This is a 2 part job. This script only does job 1.
# 1) As most river networks don't come with a bain ID we have to 
# consolidate touching lines into basins
# 2) The we can use barriers to cut these rivers 
# and calculate barrier free length (BFL)

# Time difference of 9.9 seconds for Corsica only

timedate <- paste0(format(Sys.time(), "%H%M%S"), "_", Sys.Date())

# usage: Rscript basin_rivers_sunbird.r FR_river.shp 2

library(dplyr)
library(sf)
library(rgdal)
library(rgeos)

source("st_parallel.R")

# inputs

args <- commandArgs(trailingOnly = TRUE)
cat(args, sep = "\n")

rivers <- args[1] # format rivers.shp
ncores <- as.numeric(args[2]) # format 1
epsg <- as.numeric(args[3]) # format 2192
message(paste0('Calculating basin rivers for ', rivers, ' using ', ncores, ' cores.'))

# rivers
rivers <- st_read(dsn = rivers) %>%
	st_transform(epsg)

##############
# First, turn rivers into single features per basin
# Basin rivers
##############

# Dissolve by basin ID
# Created a very small buffer polygon around the lines (0.1 metres)
# to make sure there is crossover between the lines that are connected.
message('Buffering rivers buffered to overlap so they can be dissolved...')
bufferedRivers <- st_parallel(rivers, st_buffer, ncores, dist = 0.0001)

# what time is it?
timedate <- paste0(format(Sys.time(), "%H%M%S"), "_", Sys.Date())

# set precision attribute to catch invalid geometries
message('Setting precision...')
bufferedRivers <- st_parallel(bufferedRivers, st_set_precision, ncores, 10000)

# Dissolve overlapping polygons
# This created individual polygons for each of the connected line groups.
message('Fixing geometry to stop any self intersections by buffer = 0...')
geomfix <- st_parallel(bufferedRivers, st_buffer, ncores, dist = 0)

message('Dissolving river polygons...')
dissolvedRivers <- st_parallel(geomfix, st_union, ncores)

# Multipart to singlepart
# Create a unique ID for each polygon.
message('Dissolving rivers into unique polygons for each basin...')
basinRiversPoly <- st_cast(dissolvedRivers, 'POLYGON') %>%
  as('Spatial') %>%
  st_as_sf() %>%
  tibble::rowid_to_column("basinID") %>%
  st_transform(epsg)

message('Joining unique basin ID to rivers...')
basinRivers <- st_parallel(rivers, st_join, ncores, basinRiversPoly)

basinRivers <- basinRivers %>% 
  group_by(basinID) %>% 
  summarise(ID = mean(basinID)) %>% 
  st_cast()  %>%
  select(geometry, basinID)

# what time is it?
timedate <- paste0(format(Sys.time(), "%H%M%S"), "_", Sys.Date())

# write output
message('Writing output...')
st_write(basinRiversPoly, paste0("basinRiversPoly", timedate, ".shp"), delete_layer = TRUE) # overwrites
st_write(basinRivers, paste0("basinRivers", timedate, ".shp"), delete_layer = TRUE) # overwrites

quit()