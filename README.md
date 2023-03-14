# Project-STEDI-Human-Balance-Analytics
The files above entail a data engineering project by the name "STEDI: Human Balance Analytics" by Udacity. 

The project involves creatign glue jobs to construct a collective data pipeline for a company that has been hard at work developing 
a hardware STEDI Step Trainer that:

-trains the user to do a STEDI balance exercise;
-and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
-has a companion mobile app that collects customer data and interacts with the device sensors.

## Project Objective

The objective of the project is to extract the data produced by the STEDI Step Trainer sensors and the mobile app, 
and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

Data Description

1. Customer Records (from fulfillment and the STEDI website):

AWS S3 Bucket URI - s3://cd0030bucket/customers/

contains the following fields:

serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate
customername
email
lastupdatedate
phone
sharewithfriendsasofdate
2. Step Trainer Records (data from the motion sensor):

AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

contains the following fields:

sensorReadingTime
serialNumber
distanceFromObject
3. Accelerometer Records (from the mobile app):

AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

contains the following fields:

timeStamp
serialNumber
x
y
z


Project Files:
The files uploaded are described as follows:

customer_landing.sql: This file creates a table for the raw customer data.

customer_trusted.py: This is a spark code that extracts raw customer data from AWS S3 landing zone to the trusted zone.

customer_curated.py: This is a spark code that extracts customer data from AWS S3 trusted zone to the curated zone.



accelerometer_landing.sql: Creates a table for the raw accelerometer data.

accelerometer_trusted.py: This is a spark zone that extracts raw acceleromer data from the AWS S3 landing zone to trusted zone.



step_trainer_trusted.py: This is a spark code that extracts step trainer data from AWS S3 landing zone to the trusted zone.

machine_learning_curated.py: This is a spark code that extracts data from step trainder and accelerometer truated zones and creates a machine 
learning curated data for the companies data scientists.
