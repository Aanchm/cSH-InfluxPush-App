# cSH-InfluxPush-App

## Introduction
This project was used to practice creating Command Line applications in C# and to practice LINQ after completing a pluralsight course. This also uses parallel tasks for efficiency

## Functionality
The app takes JSON and CSV files in a file directory. Any file created on the same date is grouped, and all files within the group are given an id with the format: 
DATE-ID

The files are then parsed and pushed to influx. 


