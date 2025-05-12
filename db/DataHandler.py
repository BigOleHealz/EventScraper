#! /usr/bin/python3.8
import abc, os, logging, traceback, sys
from logging import Logger

from .MetadataHandler import MetadataHandler
# from data_fetcher.eventbrite.EventbriteDataHandler import EventbriteDataHandler
# from data_fetcher.meetup.MeetupDataHandler import MeetupDataHandler

# source_handler_mapping = {
#     "eventbrite": EventbriteDataHandler,
#     "meetup": MeetupDataHandler
# }
class DataIngestionHandler(MetadataHandler, abc.ABC):
  def __init__(self, logger: Logger=None):
    if logger is None:
      logger = Logger(name="data_ingestion")
    self.logger = logger
    # self.logger = Logger(name=f"data_ingestion")
    # self.aws_handler = AWSHandler(logger=self.logger)
    super().__init__(logger=logger)

  def get_ingestions_to_be_performed(self):
    df_ingestions_to_be_performed = self.get_ingestions_to_attempt()
    print(df_ingestions_to_be_performed)
    
    # for _, row in df_ingestions_to_be_performed[:1].iterrows():
    #   try:
    #     handler = source_handler_mapping[row['source']](row=row, aws_handler=self.aws_handler)
    #     handler.run()
    #   except Exception as e:
    #     self.logger.error(msg=e)
    #     self.logger.error(msg=traceback.format_exc())

if __name__ == "__main__":
  data_ingestion_handler = DataIngestionHandler()
  data_ingestion_handler.get_ingestions_to_be_performed()