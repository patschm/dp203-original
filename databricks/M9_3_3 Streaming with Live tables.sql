-- Databricks notebook source
CREATE STREAMING LIVE TABLE events (
 CreationTime TIMESTAMP,
 Device INT,
 Series INT,
 Value INT,
 MinTemp INT,
 MaxTemp INT
)
USING DELTA
LOCATION '/delta/events'
