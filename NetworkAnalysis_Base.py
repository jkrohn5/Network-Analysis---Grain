import arcpy, os
from datetime import date, datetime,timedelta
import pandas as pd
import time

start = time.time()
print("Starting timer")
folderPath = r"D:\WorkingProjects\Network Analysis"
#setting environmental variables
ws = arcpy.env.workspace = os.path.join(folderPath,"NetworkAnalysis.gdb")
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference("NAD 1983 (2011) Contiguous USA Albers")
#arcpy.env.parallelProcessingFactor = "100%"
arcpy.env.overwriteOutput = True


print("reading files, getting things ready...")

#edges, creating in memory ones to test if calculate field works faster
input_gdb = os.path.join(folderPath, "NetworkAnalysis.gdb")
network = os.path.join(input_gdb,"FeatureDataset","Network")

#setting up loop, range determined by number of weeks between Jan 1, 2012 and Dec 31, 2022
for i in range(0,1):
#network characteristics for loop
    facilities = os.path.join(input_gdb,"CornSoy_Offset")
    incidents = os.path.join(folderPath, "NetworkAnalysis_Automated.gdb\Port_NO")
    print("found your network...")
    print("setting network values")

    print("making closest facility analysis layer")
    result_object = arcpy.na.MakeClosestFacilityAnalysisLayer(network_data_source = network,layer_name = "ClosestFacility_Loop",travel_mode ="New Travel Mode",travel_direction ="FROM_FACILITIES",number_of_facilities_to_find = 6829,line_shape = "ALONG_NETWORK",accumulate_attributes =["Dollars","Length"],generate_directions_on_solve ="NO_DIRECTIONS")
    layer_object = result_object.getOutput(0)
    
    #add facilities and incidents
    arcpy.na.AddLocations(layer_object, "Facilities", facilities, "", "")
    arcpy.na.AddLocations(layer_object, "Incidents", incidents, "", "")
    print("added facilities and incidents")

    #solve
    print("solving....")
    arcpy.na.Solve(layer_object)
    print("solved!")

    #traverse source features gives you edge type (ie road, rail, or river)
    print("getting edges...")
    arcpy.na.CopyTraversedSourceFeatures(layer_object, r"memory/","edge","junction","turns")
    routes = layer_object.listLayers()[3]
    facilities = layer_object.listLayers()[0]
    edges = r"memory\edge"
    print("adding fields")

    #Spatial join edges to ports
    print("spatial join to ports")
    spatial = os.path.join(folderPath,"Shapefiles\spatialjoin.shp")
    arcpy.analysis.SpatialJoin(os.path.join(folderPath, "Shapefiles\locks.shp"), routes, spatial, "JOIN_ONE_TO_MANY", '', '', "WITHIN_A_DISTANCE", 200)
    
    
    #adding date as field for tables so that we can use this in join later
    arcpy.management.AddField(routes, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(facilities, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(edges, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(spatial, "Analysis", "TEXT",field_length = 15)
    print("Calculating fields")
    arcpy.management.CalculateField(routes,"Analysis_Date", "'" + str(date.today()) + "'", "PYTHON3")
    arcpy.management.CalculateField(facilities,"Analysis_Date", "'" + str(date.today()) + "'", "PYTHON3")
    arcpy.management.CalculateField(edges,"Analysis_Date", "'" + str(date.today()) + "'", "PYTHON3")
    arcpy.management.CalculateField(spatial,"Analysis", "'" + str(date.today()) + "'", "PYTHON3")

    #write data tables to csv
    print("writing routes to csv")
    arcpy.TableToTable_conversion(routes, os.path.join(folderPath,"BaseOutput"),"route" + str(date.today()).replace("-","_") + ".csv")
    print("writing facilities to csv")
    arcpy.TableToTable_conversion(facilities, os.path.join(folderPath,"BaseOutput"), "facility" + str(date.today()).replace("-","_") + ".csv")
    print("writing edges to csv")
    arcpy.TableToTable_conversion(edges, os.path.join(folderPath,"BaseOutput"),"edge" + str(date.today()).replace("-","_") + ".csv")
    print("writing locks to csv")
    arcpy.TableToTable_conversion(spatial, os.path.join(folderPath,"BaseOutput"),"locks" + str(date.today()).replace("-","_") + ".csv")

    end = time.time()

    timeelapsed = (end - start)/60

    print("the process took " + str(timeelapsed) + " minutes to complete")
    
    
    


