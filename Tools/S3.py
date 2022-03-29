#----------------------------------------------------------------------------------
#
# Connect to S3, upload, download
#
#----------------------------------------------------------------------------------


import boto3
import pandas as pd

class S3():
    """
    Handles connection and communication with S3 buckets.

    """

    def __init__(self,):
        self.A = 0
        # TODO: OR do we want to store all objects in the class?
        # self.positions = df()
        # self.portfolio = df()
        # self.fx = df()
        # self.prices = df()


    def connect(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
        """Setup connection to s3 with stored Keys
        
        :param AWS_ACCESS_KEY_ID: Secret Access KEY ID
        :param AWS_SECRET_ACCESS_KEY: Secret Access key
        
        """
        try:
            self.client = boto3.client("s3",
                aws_access_key_id = AWS_ACCESS_KEY_ID,
                aws_secret_access_key = AWS_SECRET_ACCESS_KEY
            )
            print("Connected.")
        except:
            print("Error: Cannot connect.")


    def disconnect(self):
        print("disconnect")


    def listFiles(self, BUCKET_NAME, prefix):
        """List of files in Bucket while filter by specified prefix
        
        :param BUCKET_NAME: S3 Bucket to search 
        :param prefix: File prefix to filter search results
        
        """

        try:
            list_of_files = self.client.list_objects_v2(Bucket = BUCKET_NAME, Prefix = prefix)
        except:
            print("Error: Cannot list files")
        else:
            file_list = []
            for key in list_of_files['Contents']:
                file_list.append(key['Key'])

            return file_list



    def uploadFile(self, df, name, dir, bucket):
        """Reads the specified list of sheets from the control file
        and returns in the required data format
        
        :param s3: s3 client
        :param sheets: dataframe
        :param name: name of the file on s3
        :param dir: directory of file on s3
        :param bucket: name of bucket on s3
        
        """

        temp_dir = "temp/"

        filename_temp = temp_dir + name + '.tsv'
        df.to_csv(filename_temp, sep = "\t")

        filename_upload = dir + name + '.tsv'

        try:
            self.client.upload_file(
                filename_temp, 
                bucket, 
                filename_upload
            )
        except:
            print("Error - uploadFileS3" + "test")



    def downloadFile(self, BUCKET_NAME, key, filename):
        # print("Download File")
        # Downloads a single file
        try:
            self.client.download_file(BUCKET_NAME, key, filename)
        except Exception as e: print(e)
            

    def downloadFiles(self, BUCKET_NAME, file_list, columns):
        # DOwnloads several files and combines them into single dataframe.
        # columns -> list of columns to keep. 
        print("hi")

        df = pd.DataFrame(columns = columns)

        for key in file_list:

            obj = self.client.get_object(Bucket=BUCKET_NAME, Key=key)
            data = pd.read_csv(obj['Body'], index_col = 0)    

            # Subset columns
            data = data[['ISIN_FUND', 'ISIN', 'WEIGHT']]

            # Append 
            df = df.append(data)

        # Store in data format readable by both Python and R
        df = df.reset_index().drop(['index'], axis = 1)

        return df


    def readFile(self, BUCKET_NAME, key, sep = "\t"):

        obj = self.client.get_object(Bucket = BUCKET_NAME, Key = key)
        pf = pd.read_csv(obj['Body'], index_col = 0, sep = sep)  

        return pf





# S3_connect
# S3_disconnect

# S3_ListFiles

# S3_Upload

# S3_Download




def uploadFileS3(s3, df, name, dir, bucket):
    """Reads the specified list of sheets from the control file
    and returns in the required data format
    
    :param s3: s3 client
    :param sheets: dataframe
    :param name: name of the file on s3
    :param dir: directory of file on s3
    :param bucket: name of bucket on s3
    
    """

    temp_dir = "temp/"

    filename_temp = temp_dir + name + '.tsv'
    df.to_csv(filename_temp, sep = "\t")

    filename_upload = dir + name + '.tsv'

    try:
        s3.client.upload_file(
            filename_temp, 
            bucket, 
            filename_upload
        )
    except:
        print("Error - uploadFileS3" + "test")

