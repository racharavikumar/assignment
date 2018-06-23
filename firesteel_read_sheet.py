"""
Triggering cli:
 python firesteel_read_sheet.py -SP "[Spreadsheet path]" -SN "[Sheet/Tab Name]" -BN
 "[bucket name" -FN "s3 json file"
Eg:
 python firesteel_read_sheet.py -SP "/tmp/ISO10383_MIC.xls" -SN "MICs List by CC" -BN
  "dummy_bucket" -FN "asdaff.json"
"""

import os
import argparse
import time
import sys
import json
import pandas as pd
import boto3


class MICsListByCCClass(object):
    """
    Class to process spreadsheet and to create the json to aws s3
    """

    def __init__(self):
        self.details = {}

    @staticmethod
    def read_spread_sheet_fn(filepath, sheet):
        """
        Read the spread sheet using pandas module and return dataframe
        :param filepath: Spread sheet name
        :param sheet: Sheet/Tab Name
        :return: dataframe or exit
        """
        data_frame_records = None
        if os.path.isfile(filepath) and os.path.getsize(filepath) > 0:
            try:
                data_frame_records = pd.read_excel(filepath, sheet_name=sheet)
            except Exception:
                print("[ERROR] Sheet/Tab name does not exists : Provided file path '{0}'"
                      " and sheet/tab name '{1}'"
                      "".format(filepath, sheet))
                exit()
        else:
            print("[ERROR]  File does not exists OR Zero Byte file: Provided Path '{0}'"
                  "".format(filepath))
            exit()
        return data_frame_records

    @staticmethod
    def process_data_frame_fn(data_frame):
        """

        :param data_frame: sheet_data in dataframe
        :return: json object of dataframe
        """
        all_records_list = []
        total_rows = 0
        for index, row in data_frame.iterrows():
            all_records_list.append(row.to_dict())
            total_rows = index
        json_obj = json.dumps(all_records_list, indent=1)
        print "[INFO]   Total number of rows in sheet are {}".format(total_rows)
        return json_obj

    def main_fn(self):
        """
        This function triggers all other functions
        :return: status of file creation in s3 bucket
        """
        self.arguments_parser()
        data_f = self.read_spread_sheet_fn(self.details['spread_sheet_path'],
                                           self.details['sheet_name'])
        json_d = self.process_data_frame_fn(data_f)
        return self.save_to_s3_bucket(json_d)

    def save_to_s3_bucket(self, data):
        """

        :param data: json data
        :return: status of operation
        """
        s3_object = boto3.resource('s3')
        bucket = s3_object.Bucket(self.details['bucket_name'])

        try:
            bucket.put_object(
                ContentType='application/json',
                Key=self.details['s3_mics_list_cc_file'],
                Body=data)
            status = True
        except Exception as error:
            print("[ERROR]  Provide correct bucket name with correct user credentials:"
                  " TraceBack: {}".format(error))
            status = False
            exit()

        body = {"uploaded": status, "bucket": self.details['bucket_name'], "path":
                self.details['s3_mics_list_cc_file']}
        return {"body": json.dumps(body)}

    def arguments_parser(self):
        """

        :return:details of arguments passed from command line
        """
        parser = argparse.ArgumentParser(conflict_handler='resolve',
                                         description='Spread Sheet loading to S3 ')
        parser.add_argument('-SP', '--SHEET PATH', dest='spread_sheet_path', type=str,
                            help='Enter spread sheet path', required=False)
        parser.add_argument('-SN', '--SHEET NAME', dest='sheet_name', type=str,
                            help='Enter Tab or Sheet name in Spreadsheet', required=False)
        parser.add_argument('-BN', '--Bucket Name', dest='bucket_name', type=str,
                            help='Enter the bucket name in s3', required=False)
        parser.add_argument('-FN', '--S3 File Name', dest='s3_mics_list_cc_file', type=str,
                            help='Enter json file name to create in s3', required=False)
        parser.add_argument('--Help', help='display help and exit', nargs='?',
                            required=False)

        try:
            args = vars(parser.parse_args())
            if not any(args.values()):
                print('[ERROR] No arguments provided, For help run : python'
                      ' firesteel_read_sheet --help')
                exit()
            else:
                args = parser.parse_args()
                self.details['spread_sheet_path'] = args.spread_sheet_path
                self.details['sheet_name'] = args.sheet_name
                self.details['bucket_name'] = args.bucket_name
                self.details['s3_mics_list_cc_file'] = args.s3_mics_list_cc_file
        except AttributeError:
            exit_str = sys.exc_info()[0]
            print exit_str

        return self.details


if __name__ == '__main__':
    ST = time.time()
    MIC_OBJ = MICsListByCCClass()
    print MIC_OBJ.main_fn()
    print "[INFO] --- Overall time : %s " % (time.time() - ST)
