import arcpy, os
from datetime import date, datetime,timedelta
import pandas as pd
import time

arcpy.env.workspace = r"D:\WorkingProjects\Network Analysis"
folderPath = r"D:\WorkingProjects\Network Analysis"
#setting environmental variables
ws = arcpy.env.workspace = os.path.join(folderPath,"NetworkAnalysis.gdb")
arcpy.env.parallelProcessingFactor = "75%"
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference("NAD 1983 (2011) Contiguous USA Albers")
#arcpy.env.parallelProcessingFactor = "100%"
arcpy.env.overwriteOutput = True


print("reading files, getting things ready...")
middate = '01/07/2008'
#middate = '07/07/2008'
df = pd.read_csv(os.path.join(folderPath, "disaster data.csv"), dtype=str, encoding='latin-1')

#filtering data where there is no end date
df = df[(df['End Date']!= 'continuing')]
df = df[(df['End Date']!= 'Continuing')]

#Only include data for disasters we are interested in
df = df[(df['FLOOD Flash flooding'] == '1')| (df['Hurricanes Typhoons Tropical Storms'] == '1') | (df['Tornadoes'] == '1') | (df['Mudslides Debris Flows Landslides']=='1') | (df['Winter Storms Ice Storms Snow Blizzard']=='1')]

#adding leading 0 to fips code
def fix_fips(original_fips):
    if len(original_fips) < 5:
        return original_fips.zfill(5)
    else:
        return original_fips
df['FIPS'] = df['FIPS'].apply(fix_fips)

#converting date fields to datetime
df['Begin Date'] = pd.to_datetime(df['Begin Date'],format='%m/%d/%Y')
df['End Date'] = pd.to_datetime(df['End Date'],format='%m/%d/%Y')

#filter out records where begin date is after end date (bad data)
df_filter = df[df['Begin Date'] <= df['End Date']]
df2 = pd.read_csv(os.path.join(folderPath, "SoyCosts.csv"), dtype=str, skipinitialspace= True, encoding='latin-1')
df2['Week'] = pd.to_datetime(df2['Week'],format='%m/%d/%Y')

#edges, creating in memory ones to test if calculate field works faster
input_gdb = os.path.join(folderPath, "NetworkAnalysis.gdb")
network = os.path.join(input_gdb,"FeatureDataset","Network")
try:
    output_xml_file = os.path.join(folderPath, "NDTemplate.xml")
    arcpy.nax.CreateTemplateFromNetworkDataset(network, output_xml_file)
except:
    pass

#make copies of data in memory to expediate field calcs
roadedge = os.path.join(input_gdb, "FeatureDataset","Roads")
roadsInMemory = arcpy.management.CopyFeatures(roadedge, "memory/roadsInMemory")
railedge = os.path.join(input_gdb, "FeatureDataset","Rails_State")
railInMemory = arcpy.management.CopyFeatures(railedge, "memory/railInMemory")
riversedge = os.path.join(input_gdb, "FeatureDataset","Rivers_State")
riversInMemory = arcpy.management.CopyFeatures(riversedge, "memory/riversInMemory")


#setting up loop, range determined by number of weeks between Jan 1, 2012 and Dec 31, 2022
for i in range(0,772):
#for i in range(0,746):
    start = time.time()
    print("analyzing week " + str(i + 1) + " of 772")
    print("week midpoint of " + middate)
    
#extracting week for which we are looking for disasters, putting each day of week into a list
    mid_date = datetime.strptime(middate,'%m/%d/%Y')
    week_start = mid_date - timedelta(days = 3)
    week_end = mid_date + timedelta(days = 3)
    week_dates = [week_start + timedelta(days=x) for x in range((week_end-week_start).days + 1)]
    
#subset dataframe to be those records where range of begin to end falls within the given week and put fips codes into list
    ix = pd.IntervalIndex.from_arrays(df_filter['Begin Date'], df_filter['End Date'], closed='both')
    df_subset = df_filter.loc[[any(date in i for date in week_dates) for i in ix]]
    df2_subset = df2[(df2['Week'] == mid_date)]
    listoffips = df_subset['FIPS'].tolist()
    
#network characteristics for loop
    print("deleting old network")
    arcpy.management.Delete(network)
    facilities = os.path.join(input_gdb,"FeatureDataset", "SoyJunctions")
    incidents = os.path.join(folderPath, "NetworkAnalysis_Automated.gdb\Port_NO")
    print("found your network...")
    print("setting network values")
    
#get transport values for the given week
    road = float(df2_subset['Road'].iloc[0])
    rail = float(df2_subset['Rail'].iloc[0])
    river = df2_subset['River'].iloc[0]
    
#Updating edge features attribute 'Cost' then rebuilding network
    print("Calculating new costs values...")
    # Process: Start Edit
    # open an editor session
    edit = arcpy.da.Editor(input_gdb) 
    edit.startEditing(False, False)
    edit.startOperation()
    print("updating road values")
    with arcpy.da.UpdateCursor(roadsInMemory, ['Cost']) as cursor: 
        for row in cursor:
            row[0] = road
            cursor.updateRow(row)
    print("upading rail values")
    with arcpy.da.UpdateCursor(railInMemory, ['Cost']) as cursor: 
        for row in cursor:
            row[0] = rail
            cursor.updateRow(row)
    print("updatingriver values")
    with arcpy.da.UpdateCursor(riversInMemory, ['Cost']) as cursor: 
        for row in cursor:
            row[0] = river
            cursor.updateRow(row)
   # stop editing
    edit.stopOperation()
    edit.stopEditing(True)

    
#copy roads and rails from memory to gdb
    print("copying edges to gdb")
    arcpy.management.CopyFeatures(roadsInMemory, os.path.join(folderPath, "NetworkAnalysis.gdb\FeatureDataset\Roads"))
    arcpy.management.CopyFeatures(railInMemory, os.path.join(folderPath, "NetworkAnalysis.gdb\FeatureDataset\Rails_State"))
    arcpy.management.CopyFeatures(riversInMemory, os.path.join(folderPath, "NetworkAnalysis.gdb\FeatureDataset\Rivers_State"))


#build network from template
    print("rebuilding network")
    network = arcpy.nax.CreateNetworkDatasetFromTemplate(output_xml_file, os.path.join(folderPath, "NetworkAnalysis.gdb\FeatureDataset"))
    arcpy.na.BuildNetwork(network)
    
#set definition query on county feature class to fips codes of a given week
    print("setting definition query")
    barrier_fc= os.path.join(folderPath, "NetworkAnalysis_Automated.gdb\Counties_River_Erased")
    poly_barrier = arcpy.management.MakeFeatureLayer(barrier_fc)[0]
    list_string = ','.join("'{0}'".format(x) for x in listoffips)
    poly_barrier.definitionQuery = "Geoid2 in (" + list_string + ")"
    
    #print(poly_barrier.definitionQuery)
    print("making closest facility analysis layer")
    result_object = arcpy.na.MakeClosestFacilityAnalysisLayer(network_data_source = network,layer_name = "ClosestFacility_Loop",travel_mode ="New Travel Mode",travel_direction ="FROM_FACILITIES",number_of_facilities_to_find = 3241,line_shape = "ALONG_NETWORK",accumulate_attributes =["Dollars","Length"],generate_directions_on_solve ="NO_DIRECTIONS")
    layer_object = result_object.getOutput(0)
    
    #add facilities and incidents
    arcpy.na.AddLocations(layer_object, "Facilities", facilities, "", "")
    arcpy.na.AddLocations(layer_object, "Incidents", incidents, "", "")
    print("added facilities and incidents")
    if list_string:
        arcpy.na.AddLocations(layer_object, "Polygon Barriers", poly_barrier, "", "")
        print("added poly barrier")

    #solve
    print("solving....")
    arcpy.na.Solve(layer_object, "SKIP", "CONTINUE")
    print("solved!")
    
    
    #traverse source features gives you edge type (ie road, rail, or river)
    print("getting edges...")
    arcpy.na.CopyTraversedSourceFeatures(layer_object, "memory/","edge","junction","turns")
    routes = layer_object.listLayers()[3]
    facilities = layer_object.listLayers()[0]
    edges = r"memory/edge"

      #Spatial join edges to ports
    print("spatial join to ports")
    spatial = os.path.join(folderPath, "Shapefiles\spatialjoin.shp")
    arcpy.analysis.SpatialJoin(os.path.join(folderPath, "Shapefiles\locks.shp"), routes, spatial, "JOIN_ONE_TO_MANY", '', '', "WITHIN_A_DISTANCE", 50)
    
    print("adding fields")
    
    #adding date as field for tables so that we can use this in join later
    arcpy.management.AddField(routes, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(facilities, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(edges, "Analysis_Date", "TEXT",field_length = 15)
    arcpy.management.AddField(spatial, "Analysis", "TEXT",field_length = 15)
    arcpy.management.AddField(routes, "Type", "TEXT",field_length = 15)
    arcpy.management.AddField(facilities, "Type", "TEXT",field_length = 15)
    arcpy.management.AddField(edges, "Type", "TEXT",field_length = 15)
    arcpy.management.AddField(spatial, "Type", "TEXT",field_length = 15)
    print("Calculating fields")
    arcpy.management.CalculateField(routes,"Analysis_Date", "'" + middate + "'", "PYTHON3")
    arcpy.management.CalculateField(facilities,"Analysis_Date", "'" + middate + "'", "PYTHON3")
    arcpy.management.CalculateField(edges,"Analysis_Date", "'" + middate + "'", "PYTHON3")
    arcpy.management.CalculateField(spatial,"Analysis", "'" + middate + "'", "PYTHON3")
    arcpy.management.CalculateField(routes,"Type", "'Soy'", "PYTHON3")
    arcpy.management.CalculateField(facilities,"Type", "'Soy'", "PYTHON3")
    arcpy.management.CalculateField(edges,"Type", "'Soy'", "PYTHON3")
    arcpy.management.CalculateField(spatial,"Type", "'Soy'", "PYTHON3")

    #write data tables to csv
    print("writing routes to csv")
    for attempt in range(3):
        try:
            arcpy.TableToTable_conversion(routes, os.path.join(folderPath, "LoopOutput\Routes"),"Soyroute" + middate.replace("/","_") + ".csv")
        except:
            print ("This failed because arcpy is dumb, trying again...")
        else:
            break
    else:
        print( "This failed all three attempts. Check your code.")
    print("writing facilities to csv")
    for attempt in range(3):
        try:
            arcpy.TableToTable_conversion(facilities, os.path.join(folderPath, "LoopOutput\Facilities"), "Soyfacility" + middate.replace("/","_") + ".csv")
        except:
            print ("This failed because arcpy is dumb, trying again...")
        else:
            break
    else:
        print( "This failed all three attempts. Check your code.")
    
    print("writing edges to csv")
    for attempt in range(3):
        try:
            arcpy.TableToTable_conversion(edges, os.path.join(folderPath, "LoopOutput\Edges"),"Soyedge" + middate.replace("/","_") + ".csv")
        except:
            print ("This failed because arcpy is dumb, trying again...")
        else:
            break
    else:
        print( "This failed all three attempts. Check your code.")
    
    print("writing locks to csv")
    for attempt in range(3):
        try:
            arcpy.TableToTable_conversion(spatial, os.path.join(folderPath, "LoopOutput\Locks"),"Soylocks" + middate.replace("/","_") + ".csv")
        except:
            print ("This failed because arcpy is dumb, trying again...")
        else:
            break
    else:
        print( "This failed all three attempts. Check your code.")
    
    # get the layer describe object for deletion
    layer_desc = arcpy.Describe(layer_object)
    # get the layer's children
    if hasattr(layer_desc, 'children'):
        layer_path = ''
        for child in layer_desc.children:
            # the child objects should have a path property that represents the path of the closest facility feature dataset
            if hasattr(child, 'path'):
                # if we found the path, store it and break out of the loop
                if child.path != None and child.path != '':
                    layer_path = child.path
                    break
        # now delete the closest facility feature dataset if we have a path for it
        if layer_path != '':
            print(f'Deleting {layer_path}')
            arcpy.management.Delete(layer_path) 
    end = time.time()
    timeelapsed = (end - start)/60

    print("the process took " + str(timeelapsed) + " minutes to complete")
    print("finished week " + str(i + 1) + " of 772")
    
    #update date for loop
    middate = (mid_date + timedelta(days = 7)).strftime('%m/%d/%Y')
    


