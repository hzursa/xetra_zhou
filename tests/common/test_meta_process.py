"""
Test meta process Methods
"""
import os
import unittest
import pandas as pd
import boto3
from moto import mock_s3
from datetime import datetime, timedelta
import collections

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat


class TestMetaProcess(unittest.TestCase):
    """
    Testing the S3Bucket Connector class
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocket s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)

    def tearDown(self):
        """
        Executing after unittests
        """
        #mocking s3 connection stop
        self.mock_s3.stop()

    def test_return_date_list_correct1(self):
        """
        Test whether return_date_list methods can return correct results when 
        first date is earlier than meta_file first processd date
        """
        # Expected results
        # Test init
        meta_key = 'meta.csv'
        first_date = '2022-03-01'
        start = datetime.strptime('2022-03-03',MetaProcessFormat.META_DATE_FORMAT.value).date()
        today = datetime.today().date()
        d = {MetaProcessFormat.META_SOURCE_DATE_COL.value:['2022-03-01','2022-03-02','2022-03-03'],
             MetaProcessFormat.META_PROCESS_COL.value:['2022-03-01','2022-03-02','2022-03-03']}
        df_meta = pd.DataFrame(data = d)
        self.s3_bucket_conn.write_df_to_s3(df_meta,meta_key,'csv')
        # Method execution
        return_min_date, return_dates = MetaProcess.return_date_list(first_date,meta_key,self.s3_bucket_conn)
        # Test after method execution
        test_dates_list = [
                (start + timedelta(days = x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                for x in range (0, (today-start).days+1)
                ]
        self.assertEqual(collections.Counter(test_dates_list) == collections.Counter(return_dates),True)
        self.assertEqual(return_min_date,'2022-03-04')
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_correct2(self):
        """
        Test whether return_date_list methods can return correct results when 
        first date is not earlier than meta_file first processd date
        """
        # Expected results
        # Test init
        meta_key = 'meta.csv'
        first_date = '2022-03-05'
        start = datetime.strptime('2022-03-04',MetaProcessFormat.META_DATE_FORMAT.value).date()
        today = datetime.today().date()
        d = {MetaProcessFormat.META_SOURCE_DATE_COL.value:['2022-03-01','2022-03-02','2022-03-03'],
             MetaProcessFormat.META_PROCESS_COL.value:['2022-03-01','2022-03-02','2022-03-03']}
        df_meta = pd.DataFrame(data = d)
        self.s3_bucket_conn.write_df_to_s3(df_meta,meta_key,'csv')
        # Method execution
        return_min_date, return_dates = MetaProcess.return_date_list(first_date,meta_key,self.s3_bucket_conn)  
        # Test after method execution
        test_dates_list = [
                (start + timedelta(days = x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                for x in range (0, (today-start).days+1)
                ]
        self.assertEqual(collections.Counter(test_dates_list) == collections.Counter(return_dates),True)
        self.assertEqual(return_min_date,'2022-03-05')
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_upload_meta_file(self):
        """
        Test whether meta_file can be correctly updated after process
        """
        # Expected results
        # Test init
        meta_key = 'meta.csv'
        first_date = '2022-03-01'
        start = datetime.strptime('2022-03-01',MetaProcessFormat.META_DATE_FORMAT.value).date()
        today = datetime.today().date()
        d = {MetaProcessFormat.META_SOURCE_DATE_COL.value:['2022-03-01','2022-03-02','2022-03-03'],
             MetaProcessFormat.META_PROCESS_COL.value:['2022-03-01','2022-03-02','2022-03-03']}
        df_meta = pd.DataFrame(data = d)
        self.s3_bucket_conn.write_df_to_s3(df_meta,meta_key,'csv')
        return_min_date, return_dates = MetaProcess.return_date_list(first_date,meta_key,self.s3_bucket_conn)  
        test_dates_list =[(start+ timedelta(days = x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                for x in range (0, (today-start).days+1)
                ]
        # Method execution
        report_date_list = [date for date in return_dates if date >= return_min_date]
        MetaProcess.update_meta_file(meta_key,report_date_list,self.s3_bucket_conn)
        # Test after method execution
        df_updated = self.s3_bucket_conn.read_csv_to_df(meta_key)
        updated_dates_list = [dates for dates in df_updated[MetaProcessFormat.META_SOURCE_DATE_COL.value].to_numpy()]
        self.assertEqual(test_dates_list, updated_dates_list)
        self.assertEqual(return_min_date,'2022-03-04')
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


if __name__ == "__main__":
    unittest.main()