# 23/05/2019
# Josh Jones
# This is a 2 part job. This script only does job 2.
# 1) As most river networks don't come with a basin ID we have to 
# consolidate touching lines into basins
# 2) The we can use barriers to cut these rivers 
# and calculate barrier free length (BFL)

# Completes in 34 seconds for Corsica using 2 cores

# usage: Rscript bfl_sunbird.R FR_rivers.shp FR_barriers.shp 2192 2

library(plyr)
library(dplyr)
library(sf)
library(parallel)
library(lwgeom)
library(doParallel)
library(foreach)

time <- Sys.time()
message('Step 1. Start by importing data and joining attribute IDs at ', time)

# # both versions of the st_parallel function
# source("st_parallel.R")

# inputs
args <- commandArgs(trailingOnly = TRUE)
cat(args, sep = "\n")
basinRivers <- args[1] # format basin_rivers.shp, make sure it's basin_rivers.shp !!!!!!
barriers <- args[2] # format barriers.shp # snapped barriers because snapping takes too long in sf - use QGIS (SAGA)
epsg <- as.numeric(args[3]) # format 2192
ncores <- as.numeric(args[4]) # format 1

message(paste0('Calculating barrier free length for ', basinRivers, ' using ', ncores, ' cores.'))

# rivers
basinRivers <- st_read(dsn = basinRivers) %>% 
  st_transform(epsg)

# barriers
barriers <- st_read(dsn = barriers) %>%
  select(1) %>% # only need one column
  st_transform(epsg)

# study site boundary buffered to 10 km
boundary <- 'FR_boundary_10km.shp'
boundary <- st_read(dsn = boundary) %>% 
  st_transform(epsg)



# ##############
# # Second, cut rivers with points
# ##############

# Functions
# Snap barriers to river
st_snap_points = function(x, y, max_dist = 1000) {

  if (inherits(x, "sf")) n = nrow(x)
  if (inherits(x, "sfc")) n = length(x)

  out = do.call(c,
                lapply(seq(n), function(i) {
                  nrst = st_nearest_points(st_geometry(x)[i], y)
                  nrst_len = st_length(nrst)
                  nrst_mn = which.min(nrst_len)
                  if (as.vector(nrst_len[nrst_mn]) > max_dist) return(st_geometry(x)[i])
                  return(st_cast(nrst[nrst_mn], "POINT")[2])
                })
  )
  return(out)
}

# A helper function that erases all of y from x: 
st_erase = function(x, y) st_difference(x, st_union(y))

erase <- function(bar, riv) {
  buffBarriers <- st_buffer(bar, dist = 0.0001)
  diffRivers <- st_erase(riv, buffBarriers) %>% st_transform(epsg)
  return(diffRivers)
}

# Perform a minimal snap to account for the shape of the network
# introduced by modelling it
# message('Snapping barriers to network...')
# barriers <- st_snap_points(barriers, basinRivers, 50)

message('Joining basinID to barriers...')
barriers <- mutate(barriers, river_id = st_nearest_feature(geometry, basinRivers))
barriers <- mutate(barriers, ID_DRAIN = basinRivers[river_id,][['ID_DRAIN']])

time <- Sys.time()
message('Step 2. Fragmenting rivers in parallel at ', time)

message('Setting up cluster...')
# Add basinID to barriers
cl <- makeCluster(ncores)
clusterExport(cl, c("barriers", "basinRivers", "st_erase", "erase", "epsg", "ncores"))
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(dplyr))
clusterEvalQ(cl, library(lwgeom))

message('Fragmenting rivers...')

basinRiversBarr <- basinRivers %>% 
  filter(ID_DRAIN %in% unique(barriers$ID_DRAIN)) %>% 
  mutate(ID_DRAIN = as.numeric(ID_DRAIN)) %>%
  arrange(ID_DRAIN)

basinRiversNoBarr <- basinRivers %>% 
  filter(!ID_DRAIN %in% unique(basinRiversBarr$ID_DRAIN)) %>%
  st_transform(epsg)

barriers_arranged <- barriers %>%
  mutate(ID_DRAIN = as.numeric(ID_DRAIN)) %>%
  arrange(ID_DRAIN)

# group barriers to split them by group
barriers_grouped <- barriers_arranged %>% group_by(ID_DRAIN)

# this is faster but not sure about CPU efficiency
diffRiversBarr <- foreach(bar = group_split(barriers_grouped), 
  riv = split(basinRiversBarr, rep(1:nrow(basinRiversBarr), each = 1))) %dopar% {
  erase(bar, riv)
}

stopCluster(cl)

message('Binding fragmented rivers to rivers without barriers...')
diffRivers <- st_as_sf(as.data.frame(bind_rows(dlply(as.data.frame(basinRiversNoBarr), 1, c), 
                                       diffRiversBarr)))

time <- Sys.time()
message('Step 3. Buffering and fixing things at ', time)

# Create river polygons and split them into seperate segments
message('Buffering differenced rivers...')
buffRivers <- st_buffer(diffRivers, 0.00009)
# buffRivers <- do.call(rbind.data.frame, buffRivers)

# Clip river polygons to study site boundary
message('Clipping buffered rivers...')
st_crs(buffRivers) <- epsg
clipRiver <- st_crop(buffRivers, boundary)

# Fix geom just in case
message('Fixing invald geometry...')
geomfix <-  clipRiver %>% st_set_precision(1000000) %>%
  st_make_valid()
# Group by basin so we can iterate over basins in parallel
geomfixgrouped <- geomfix %>% group_by(basinID)

time <- Sys.time()
message('Step 4. Dissolving rivers at ', time)

# Dissolving buffered and clipped rivers
message('Setting up cluster for st_union...')
# Add basinID to barriers
cl <- makeCluster(ncores)
clusterExport(cl, c("geomfixgrouped"))
clusterEvalQ(cl, library(sf))

message('Dissolving river polygons...')
dissRivers_ls <- foreach(basin = group_split(geomfixgrouped)) %dopar% {
  st_union(basin)
}

stopCluster(cl)

time <- Sys.time()
message('Step 5. Recombining results at ', time)

# Turn a list into an sf data.frame
message('Recombining dissolved river polygons...')
dissRivers <- st_as_sf(as.data.frame(do.call(rbind, dissRivers_ls)))

# Fix the geometry column name and projection
geomName <- colnames(dissRivers[1])
dissRivers <- dissRivers %>%
  rename(geometry = geomName)
st_crs(dissRivers) <- epsg
rownames(dissRivers) <- NULL

# Turn features with multiple parts into many seperate parts.
# In QGIS this is called multipart to singlepart.
# In sf it's st_cast().
# This layer is used to identify each of the continuous sections
# between barriers.
message('Casting dissolved/unioned river polygons into singlepart polygons...')
singlepartpoly <- st_cast(dissRivers, "MULTIPOLYGON") %>% 
  st_cast("POLYGON") %>%
  mutate(fragid = as.factor(row_number()))

message('Casting differenced river lines into singlepart lines...')
singlepartline <- st_cast(st_cast(diffRivers, "MULTILINESTRING"),"LINESTRING")
st_crs(singlepartline) <- epsg

# Bring attributes back together
message('Joining attributes from frag and basin rivers...')
fragLines <- st_join(singlepartline, singlepartpoly['fragid'])

# This step is for the French RHT river network that has some inconsitencies
# Now's the time to weed out any topological errors in the river network
# Use QGIS or ArcMap
fragLines <- fragLines %>%
  mutate(basinID = ifelse(basinID == 618, 1065, basinID),
         basinID = ifelse(basinID == 617, 1086, basinID))

# calculate components of BFL
message('Generating final output...')
fragBasin <- fragLines %>%
  mutate(seglen = st_length(fragLines)) %>%
  group_by(fragid) %>%
  summarise(basinID = mean(basinID),
            fraglen = sum(seglen))

basinlen <- fragBasin  %>%
  st_drop_geometry() %>%
  group_by(basinID) %>%
  summarise(basinlen = sum(fraglen))

BFLS <- fragBasin %>% 
  left_join(basinlen, by = 'basinID') %>% 
  mutate(BFLS = as.numeric(fraglen/basinlen))

# write output
message('Writing output...')
timedate <- paste0(format(Sys.time(), "%H%M%S"), "_", Sys.Date())
st_write(BFLS, paste0("BFLSbyseg", timedate, ".shp"), delete_layer = TRUE) # overwrites

time <- Sys.time()
message('Finished at ', time)

quit()