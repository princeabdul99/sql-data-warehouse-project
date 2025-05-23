/*
==============================================================================================
Create Database and Schemas
==============================================================================================
Script Purpose:
  This script creates a new database name 'Datawarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the scripts sets up three 
  schemas within the database: 'bronze', 'silver', 'gold'.


WARNING:
  Running this script will drop the entire 'Datawarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure you have
  proper backups before running this script.
*/



USE master;
GO

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')  
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;
GO

-- Create the 'Datawarehouse' Database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

-- Create Schemas 
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
