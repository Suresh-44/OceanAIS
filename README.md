# OceanAIS


# Supply Chain Project

#### A brief description of the data 
     
     The dataset related to different vessels AIS (Automatic Identification System) data. The dataset consists of static and dynamic fields of a ship.

     Static Fields: The static information stay the same for the whole trip of one ship.
        MMSI: unique 9-digit identification code of the ship - numeric
        VesselName: name of the ship - string
        IMO: unique 7-digit international identification number, that remains unchanged after the transfer of the ship’s registration to another country - numeric
        CallSign: unique callsign of the ship - string
        Draught: vertical distance between the waterline and the bottom of the hull of the ship, in meters. For one ship, varies with the load of the ship and the density of the water - numeric
        VesselType: type of the ship, numerically coded, see here for details - numeric
        Length: length of the ship, in meters - numeric
        Width: width of the ship, in meters - numeric
     Dynamic Fields: 
        LAT: latitude of the ship (in degree: [-90 ; 90], negative value represents South, 91 indicates ‘not available’) - numeric
        LON: longitude of the ship (in degree: [-180 ; 180], negative value represents West, 181 indicates ‘not available’) - numeric
        SOG: speed over ground, in knots - numeric
        COG: course over ground, direction relative to the absolute North (in degree: [0 ; 359]) - numeric
        Heading: heading of the ship (in degree: [0 ; 359], 511 indicates ‘not available’) - numeric
        Status: status of the ship - string

#### About Code

	 - I have used apache spark with scala programming language to implement the code.
	 - All queries have been externalised into config file called queries.json which is under  src\main\resources\config\queries.json
	 - main class is under src\main\scala\com\supplychainproject\OceanAISMain.scala


## Questions And Answers

#### Question 2 . Which file format you choose?

Answer: I choose parquet format over json due to the following reasons
    1. No need to explicitly provide schema since parquet format has metadata to determine column names and data types.
    2. Parquet is a columnar format they can be highly compressed. This is ideal for solving big data problems.
    3. Apache spark has built-in support for parquet which makes it easy to simply take and save a file in storage.
    4. Parquet provides very good compression using compression formats like snappy.

#### Question 3. What is(are) the main time period(s) in the data?

     Answer: Time period is calculated in hours as shown below.
		    +---------+-------------------+-------------------+-------+----------------------+-----------------+
		    |mmsi     |start_time         |end_time           |navcode|navdesc               |TimePeriodInHours|
		    +---------+-------------------+-------------------+-------+----------------------+-----------------+
		    |201006210|2020-03-23 05:58:34|2020-03-23 21:27:54|0      |Under Way Using Engine|15.49            |
		    |201006220|2020-03-21 05:30:42|2020-03-28 05:19:03|16     |Unknown               |167.81           |
		    |201006220|2019-03-23 05:18:40|2019-03-30 05:07:27|0      |Under Way Using Engine|167.81           |
		    |201208170|2020-03-23 15:29:06|2020-03-23 15:29:06|0      |Under Way Using Engine|0.0              |
		    |201211180|2019-03-25 05:34:32|2020-03-28 05:19:08|0      |Under Way Using Engine|8855.74          |
		    |201407006|2019-03-23 19:40:43|2020-03-24 10:28:11|16     |Unknown               |8798.79          |
		    |201449416|2019-03-26 14:10:18|2019-03-27 00:40:40|16     |Unknown               |10.51            |
		    |201551323|2019-03-28 12:58:48|2019-03-28 13:27:16|16     |Unknown               |0.47             |
		    |201607270|2019-03-23 09:07:06|2020-03-26 12:22:22|16     |Unknown               |8859.25          |
		    |201706102|2019-03-28 04:10:16|2019-03-28 11:45:14|16     |Unknown               |7.58             |
		    |201801112|2019-03-25 07:00:44|2019-03-29 08:59:33|16     |Unknown               |97.98            |
		    |201801115|2020-03-26 07:16:55|2020-03-26 09:04:40|16     |Unknown               |1.8              |
		    |201801117|2019-03-26 06:38:21|2019-03-27 06:57:02|16     |Unknown               |24.31            |
		    |201801216|2019-03-23 06:04:08|2019-03-29 19:33:42|16     |Unknown               |157.49           |
		    |201804190|2019-03-25 11:23:27|2019-03-29 09:26:05|5      |Moored                |94.04            |
		    |201804190|2020-03-21 05:26:19|2020-03-28 04:31:18|0      |Under Way Using Engine|167.08           |
		    |201908051|2020-03-21 05:29:08|2020-03-25 12:40:43|16     |Unknown               |103.19           |
		    |202003902|2020-03-23 10:10:44|2020-03-28 05:16:18|16     |Unknown               |115.09           |
		    |205549000|2019-03-30 00:42:12|2019-03-30 04:18:32|0      |Under Way Using Engine|3.61             |
		    |205717000|2019-03-26 22:34:02|2019-03-27 06:04:22|0      |Under Way Using Engine|7.51             |
		    +---------+-------------------+-------------------+-------+----------------------+-----------------+

#### Question 4: Which are the top three most sparse variables?

Answer: 
        1. cargoDetails column has 3140605 null values.
        2. imo column has 2646208 null and zero values.
        3. destination column has 1136579 null values.   
#### Question 5: What region(s) of the world and ocean port(s) does this data represent? Provide evidence to justify your answer?

     Answer: By using vesselDetails.flagCountry and port.name columns the following result has been calculated 
		    +---------+-------+-----------+
		    |mmsi     |country|port       |
		    +---------+-------+-----------+
		    |201006210|Albania|SHANGHAI PT|
		    |201006220|Albania|SHANGHAI PT|
		    |201208170|Albania|SHANGHAI PT|
		    |201211180|Albania|SHANGHAI PT|
		    |201407006|Albania|SHANGHAI PT|
		    |201449416|Albania|SHANGHAI PT|
		    |201551323|Albania|SHANGHAI PT|
		    |201607270|Albania|SHANGHAI PT|
		    |201706102|Albania|SHANGHAI PT|
		    |201801112|Albania|SHANGHAI PT|
		    |201801115|Albania|SHANGHAI PT|
		    |201801117|Albania|SHANGHAI PT|
		    |201801216|Albania|SHANGHAI PT|
		    |201804190|Albania|SHANGHAI PT|
		    |201908051|Albania|SHANGHAI PT|
		    |202003902|Andorra|SHANGHAI PT|
		    |205549000|Belgium|SHANGHAI PT|
		    |205717000|Belgium|SHANGHAI PT|
		    |205791000|Belgium|SHANGHAI PT|
		    |205792000|Belgium|SHANGHAI PT|
		    +---------+-------+-----------+

#### Question 6: Provide a frequency tabulation of the various Navigation Codes & Descriptions (i.e., navCode & NavDesc)?

     Answer: Frequency results shown below.
		    +-------+-----------------------------+---------+
		    |navcode|navdesc                      |frequency|
		    +-------+-----------------------------+---------+
		    |16     |Unknown                      |1357985  |
		    |0      |Under Way Using Engine       |1063676  |
		    |5      |Moored                       |554133   |
		    |1      |At Anchor                    |426433   |
		    |15     |Not Defined                  |29330    |
		    |8      |Underway Sailing             |24889    |
		    |3      |Restricted Manoeuvrability   |8237     |
		    |2      |Not Under Command            |3471     |
		    |4      |Constrained By Her Draught   |1483     |
		    |9      |Reserved For Future Amendment|1359     |
		    |11     |Reserved For Future Use      |1277     |
		    |13     |Reserved For Future Use      |875      |
		    |6      |Aground                      |401      |
		    |7      |Engaged In Fishing           |321      |
		    |12     |Reserved For Future Use      |7        |
		    +-------+-----------------------------+---------+

#### Question 7: For MMSI = 205792000, provide the following report: 
    a.   Limit the data to only the TOP 5 Navigation Codes based from the response to question 6?
         Answer: As there were only 3 navigation codes for MMSI = 205792000. Therefore following results are shown. 

		         +-------+----------------------+---------+
		         |navcode|navdesc               |frequency|
		         +-------+----------------------+---------+
		         |1      |At Anchor             |48       |
		         |5      |Moored                |56       |
		         |0      |Under Way Using Engine|80       |
		         +-------+----------------------+---------+

    b. Provide the final state for each series of contiguous events with the same Navigation Code; series may be interrupted by other series, but each contiguous series must be its own record?
       Answer: 
		       +---------+-------------------+-------------------+-------+----------------------+
		       |mmsi     |start_time         |end_time           |navcode|navdesc               |
		       +---------+-------------------+-------------------+-------+----------------------+
		       |205792000|2020-03-23 12:53:38|2020-03-23 13:28:32|5      |Moored                |
		       |205792000|2020-03-23 13:49:31|2020-03-24 03:19:31|1      |At Anchor             |
		       |205792000|2020-03-24 03:39:20|2020-03-24 11:27:21|0      |Under Way Using Engine|
		       |205792000|2020-03-24 11:38:20|2020-03-25 09:02:21|5      |Moored                |
		       |205792000|2020-03-25 09:31:26|2020-03-25 15:21:28|0      |Under Way Using Engine|
		       +---------+-------------------+-------------------+-------+----------------------+
    
    c.   Final report should include at least the following fields/columns:
    i.   mmsi = the MMSI of the vessel
    ii.   timestamp = the timestamp of the last event in that contiguous series
    iii.   Navigation Code = the navigation code (i.e., navigation.navCode)
    iv.   Navigation Description = the navigation code description (i.e., navigation.navDesc)
    v.   lead time (in Milliseconds) = the time difference in milliseconds between the last and first timestamp of that particular series of the same contiguous navigation codes

         Answer:
            +---------+-------------------+-------------------+-------+----------------------+---------+
            |mmsi     |start_time         |end_time           |navcode|navdesc               |lead_time|
            +---------+-------------------+-------------------+-------+----------------------+---------+
            |205792000|2020-03-23 12:53:38|2020-03-23 13:28:32|5      |Moored                |2094000  |
            |205792000|2020-03-23 13:49:31|2020-03-24 03:19:31|1      |At Anchor             |48600000 |
            |205792000|2020-03-24 03:39:20|2020-03-24 11:27:21|0      |Under Way Using Engine|28081000 |
            |205792000|2020-03-24 11:38:20|2020-03-25 09:02:21|5      |Moored                |77041000 |
            |205792000|2020-03-25 09:31:26|2020-03-25 15:21:28|0      |Under Way Using Engine|21002000 |
            +---------+-------------------+-------------------+-------+----------------------+---------+

#### Question 8: For MMSI = 413970021, provide the same report as number 7
    a.   Do you agree with the Navigation Code(s) and Description(s) for this particular vessel?
    i. If you do agree, provide an explanation why you agree.
    ii. If you do not agree, provide an explanation why do disagree. Additionally, if you do not agree, what would you change it to and why?
        Answer: (i) The following result shows the frequency of navigation descriptions of mmsi = 413970021.
        			I do not agree with the navigation code and navigation description. The reason is there is no navigation code which is 16 in AIS navigation status.
			        +-------+-------+---------+
			        |navcode|navdesc|frequency|
			        +-------+-------+---------+
			        |16     |Unknown|393      |
			        +-------+-------+---------+

			        (ii) The following result shows the timestamp of the last event.
			        +---------+-------------------+-----------------------+-------+-------+
			        |mmsi     |start_time         |timestamp_of_last_event|navcode|navdesc|
			        +---------+-------------------+-----------------------+-------+-------+
			        |413970021|2019-03-23 05:19:54|2019-03-30 01:46:09    |16     |Unknown|
			        +---------+-------------------+-----------------------+-------+-------+

			        (iii) The following result shows the time difference in milliseconds as lead time between the contiguous event.
			        +---------+-------------------+-----------------------+-------+-------+---------+
			        |mmsi     |start_time         |timestamp_of_last_event|navcode|navdesc|lead_time|
			        +---------+-------------------+-----------------------+-------+-------+---------+
			        |413970021|2019-03-23 05:19:54|2019-03-30 01:46:09    |16     |Unknown|591975000|
			        +---------+-------------------+-----------------------+-------+-------+---------+

        			

##### Question 9. For each of the time period(s) from item three, provide a tabulation of the top 10 series of vessel navigation code/description ordered states?
      Answer: Top 10 series of navigation codes or descriptions

		      +---------+-------------------+-------------------+-------+----------------------+-----------------+
		      |mmsi     |start_time         |end_time           |navcode|navdesc               |TimePeriodInHours|
		      +---------+-------------------+-------------------+-------+----------------------+-----------------+
		      |412429036|2019-03-23 05:15:59|2020-03-28 05:23:48|16     |Unknown               |8904.13          |
		      |413374340|2019-03-23 05:15:41|2020-03-28 05:23:11|0      |Under Way Using Engine|8904.13          |
		      |413766406|2019-03-23 05:13:55|2020-03-28 05:21:44|16     |Unknown               |8904.13          |
		      |413369130|2019-03-23 05:15:34|2020-03-28 05:22:36|0      |Under Way Using Engine|8904.12          |
		      |412428337|2019-03-23 05:16:16|2020-03-28 05:22:44|16     |Unknown               |8904.11          |
		      |413794296|2019-03-23 05:14:01|2020-03-28 05:20:25|16     |Unknown               |8904.11          |
		      |413761439|2019-03-23 05:15:56|2020-03-28 05:22:15|16     |Unknown               |8904.11          |
		      |413777558|2019-03-23 05:16:02|2020-03-28 05:22:31|16     |Unknown               |8904.11          |
		      |412428877|2019-03-23 05:16:56|2020-03-28 05:22:42|16     |Unknown               |8904.1           |
		      |413787551|2019-03-23 05:16:03|2020-03-28 05:21:52|16     |Unknown               |8904.1           |
		      +---------+-------------------+-------------------+-------+----------------------+-----------------+  

##### Question 10: Using the results from item 9, compare the volume of each vessel navigation code/description ordered states for each time period(s) from item three.
    a.   Which increased/decreased?
         Answer: 
		        +---------+-------------------+-------------------+-------+----------------------+---------+
		        |mmsi     |start_time         |end_time           |navcode|navdesc               |state    |
		        +---------+-------------------+-------------------+-------+----------------------+---------+
		        |201006210|2020-03-23 05:58:34|2020-03-23 21:27:54|0      |Under Way Using Engine|decreased|
		        |201006220|2019-03-23 05:18:40|2019-03-30 05:07:27|0      |Under Way Using Engine|decreased|
		        |201006220|2020-03-21 05:30:42|2020-03-28 05:19:03|16     |Unknown               |decreased|
		        |201208170|2020-03-23 15:29:06|2020-03-23 15:29:06|0      |Under Way Using Engine|decreased|
		        |201211180|2019-03-25 05:34:32|2020-03-28 05:19:08|0      |Under Way Using Engine|decreased|
		        |201407006|2019-03-23 19:40:43|2020-03-24 10:28:11|16     |Unknown               |decreased|
		        |201449416|2019-03-26 14:10:18|2019-03-27 00:40:40|16     |Unknown               |decreased|
		        |201551323|2019-03-28 12:58:48|2019-03-28 13:27:16|16     |Unknown               |decreased|
		        |201607270|2019-03-23 09:07:06|2020-03-26 12:22:22|16     |Unknown               |decreased|
		        |201706102|2019-03-28 04:10:16|2019-03-28 11:45:14|16     |Unknown               |decreased|
		        |201801112|2019-03-25 07:00:44|2019-03-29 08:59:33|16     |Unknown               |decreased|
		        |201801115|2020-03-26 07:16:55|2020-03-26 09:04:40|16     |Unknown               |decreased|
		        |201801117|2019-03-26 06:38:21|2019-03-27 06:57:02|16     |Unknown               |decreased|
		        |201801216|2019-03-23 06:04:08|2019-03-29 19:33:42|16     |Unknown               |decreased|
		        |201804190|2019-03-25 11:23:27|2019-03-29 09:26:05|5      |Moored                |decreased|
		        |201804190|2020-03-21 05:26:19|2020-03-28 04:31:18|0      |Under Way Using Engine|decreased|
		        |201908051|2020-03-21 05:29:08|2020-03-25 12:40:43|16     |Unknown               |decreased|
		        |202003902|2020-03-23 10:10:44|2020-03-28 05:16:18|16     |Unknown               |decreased|
		        |205549000|2019-03-30 00:42:12|2019-03-30 04:18:32|0      |Under Way Using Engine|decreased|
		        |205717000|2019-03-26 22:34:02|2019-03-27 06:04:22|0      |Under Way Using Engine|decreased|
		        +---------+-------------------+-------------------+-------+----------------------+---------+

#### Question 11. For each of the time period(s) from item three and using only the “At Anchor” and “Moored” navigation descriptions, quantify the average “dwell”?
     Answer: Average dwell using only 'At Anchor' and 'Moored' navigation descriptions.
            +---------+---------------------+
            |mmsi     |average_dwell_InHours|
            +---------+---------------------+
            |201804190|94.04                |
            |205717000|32.83                |
            |205791000|26.56                |
            |205792000|11.83                |
            |209083000|14.33                |
            |209087000|12.68                |
            |209116000|14.99                |
            |209251000|12.22                |
            |209282000|18.99                |
            |209287000|30.8                 |
            |209366000|87.86                |
            |209370000|10.75                |
            |209384000|167.55               |
            |209444000|67.22                |
            |209539000|29.49                |
            |209540000|10.8                 |
            |209596000|0.0                  |
            |209782000|106.61               |
            |209906000|13.05                |
            |209995000|16.6                 |
            +---------+---------------------+

   
## Instructions To Build and Run

#### Instructions to Build 
     The build tool used in this project is sbt (scala build tool).
     	The following command is used to clean the old jars 

     		* sbt clean - this command is used to clean previous jars

 		The following command is used to build fat jar

     		* sbt assembly - this command is used to build jar file

#### Instructions to Run
     The following command is used to run on the spark cluster

	   spark-submit \
	  --master <master-url> \
	  --deploy-mode <deploy-mode> \
	  --driver-memory <value>g \
	  --executor-memory <value>g \
	  --executor-cores <number of cores>  \
	  --class com.SupplyChainProject.OceanAISMain \
	  <application-jar-path> \
	  <input-path>

	  * please replace the above command with your values.
