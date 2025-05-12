#! /usr/bin/python3.8
import abc, os, json, traceback, sys

from DataHandler import DataIngestionHandler

def run():
    data_ingestion_handler = DataIngestionHandler()
    data_ingestion_handler.get_ingestions_to_be_performed()

if __name__ == "__main__":
    run()