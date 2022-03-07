"""
Test S3 Bucket Connector Methods
"""
import os
import unittest
import pandas as pd
import boto3
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
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

    def test_list_files_in_prefix_ok(self):
        """
        Test the list_file_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # Test init
        # upload test content
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content,Key=key1_exp)     
        self.s3_bucket.put_object(Body=csv_content,Key=key2_exp)   

        #Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        #Tests after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp,list_result)
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Test the list_file_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Expected results
        # Test init
        prefix = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix)
        # Test after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df_get_correct_result(self):
        """
        Test the list_file_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Expected results
        """the result of method is equal to the one we uploaded"""
        prefix_exp = 'prefix/'
        key_exp = f'{prefix_exp}test.csv'

        # Test init
        csv_content = """col1,col2
valA,valB"""
        d = {'col1':['valA'], 'col2':['valB']}
        df_origin = pd.DataFrame(data = d)
        self.s3_bucket.put_object(Body=csv_content,Key=key_exp)
        #Method execution
        df_test = self.s3_bucket_conn.read_csv_to_df(key_exp)
        #Tests after method execution
        self.assertEqual(df_origin.equals(df_test),True)
        #self.assertEqual(df_origin.iloc[0,0],df_test.iloc[0,0])
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_write_correct_result(self):
        """
        Test the list_file_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Expected results: when we download it again we can get exactly same result.
        # Test init
        prefix_exp = 'prefix/'
        key_exp = f'{prefix_exp}test.csv'
        d = {'col1':['valA'], 'col2':['valB']}
        df_origin = pd.DataFrame(data = d)
        #Method execution
        self.s3_bucket_conn.write_df_to_s3(df_origin,key_exp,'csv')
        #Tests after method execution
        df_test = self.s3_bucket_conn.read_csv_to_df(key_exp)
        self.assertEqual(df_origin.equals(df_test),True)
        #Cleanup after test
        self.s3_bucket.delete_objects(
            Delete = {
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )


if __name__ == "__main__":
    unittest.main()