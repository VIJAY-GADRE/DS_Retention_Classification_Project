import json
from os import listdir
import shutil
import pandas as pd
from datetime import datetime
import os
from apps.database.database_operation import DatabaseOperation
from apps.core.logger import Logger


class LoadValidate:
    """
    *****************************************************************************
    *
    * filename:       LoadValidate.py
    * version:        1.0
    * author:         VIJAY-GADRE
    * creation date:  06-MAY-2023
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class to load, validate, and transform the data
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'LoadValidate', mode)
        self.dbOperation = DatabaseOperation(self.run_id, self.data_path, mode)

    def values_from_schema(self, schema_file):
        """
        * method: values_from_schema
        * description: To read schema file
        * return: column_names, Number of Columns
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   schema_file:
        """
        try:
            self.logger.info('Start of Reading values from Schema...')
            with open('apps/database/' + schema_file + '.json', 'r') as f:
                dic = json.load(f)
                f.close()
            column_names = dic['ColName']
            number_of_columns = dic['NumberofColumns']
            self.logger.info('End of Reading values from Schema!!!')

        except ValueError:
            self.logger.exception('ValueError raised while Reading values from Schema')
            raise ValueError
        except KeyError:
            self.logger.exception('KeyError raised while Reading values from Schema')
            raise KeyError
        except Exception as e:
            self.logger.exception('Exception raised while Reading values from Schema: %s' % e)
            raise e
        return column_names, number_of_columns

    def validate_column_length(self, number_of_columns):
        """
        * method: validate_column_length
        * description: To validate the number of columns in the CSV file
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   number_of_columns:
        """
        try:
            self.logger.info('Start of Validating Column Length...')
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path + '/' + file)
                if csv.shape[1] == number_of_columns:
                    pass
                else:
                    shutil.move(self.data_path + '/' + file, self.data_path + '_rejects')
                    self.logger.info("Invalid Columns Length :: %s" % file)

            self.logger.info('End of Validating Column Length!!!')

        except OSError:
            self.logger.exception('OSError raised while Validating Column Length')
            raise OSError
        except Exception as e:
            self.logger.exception('Exception raised while Validating Column Length: %s' % e)
            raise e

    def validate_missing_values(self):
        """
        * method: validate_missing_values
        * description: To validate the missing values present in any column of the CSV file.
        *              If all the values are missing, the file is not suitable for processing. [Moved to Bad File]
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        try:
            self.logger.info('Start of Validating Missing Values...')
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path + '/' + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move(self.data_path + '/' + file, self.data_path + '_rejects')
                        self.logger.info('All Missing Values in Column :: %s' % file)
                        break
            self.logger.info('End of Validating Missing Values!!!')

        except OSError:
            self.logger.exception('OSError raise while Validating Missing Values')
            raise OSError
        except Exception as e:
            self.logger.exception('Exception raised while Validating Missing Values :: %s' % e)
            raise e

    def replace_missing_values(self):
        """
        * method: replace_missing_values
        * description: To replace the missing values in columns with "NULL"
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        try:
            self.logger.info('Start of Replacing Missing Values with NULL...')
            files = [f for f in listdir(self.data_path)]
            for file in files:
                csv = pd.read_csv(self.data_path + '/' + file)
                csv.fillna('NULL', inplace=True)
                csv.to_csv(self.data_path + '/' + file, index=None, header=True)
                self.logger.info('%s: File Transformed Successfully!!!' % file)
            self.logger.info('End of Replacing Missing Values with NULL!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Replacing Missing Values with NULL: %s' % e)
            raise e

    def archive_old_files(self):
        """
        * method: archive_old_files
        * description: To archive rejected files
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            self.logger.info('Start of Archiving Old Rejected Files...')
            source = self.data_path + '_rejects/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/reject_' + str(date) + '_' + str(time)
                files = os.listdir(source)
                for file in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if file not in os.listdir(dest):
                        shutil.move(source + file, dest)
            self.logger.info('End of Archiving Old Rejected Files!!!')

            self.logger.info('Start of Archiving Old Validated Files...')
            source = self.data_path + '_validation/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/validation_' + str(date) + '_' + str(time)
                files = os.listdir(source)
                for file in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if file not in os.listdir(dest):
                        shutil.move(source + file, dest)
            self.logger.info('End of Archiving Old Validated Files!!!')

            self.logger.info('Start of Archiving Old Processed Files...')
            source = self.data_path + '_processed/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/processed_' + str(date) + '_' + str(time)
                files = os.listdir(source)
                for file in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if file not in os.listdir(dest):
                        shutil.move(source + file, dest)
            self.logger.info('End of Archiving Old Processed Files!!!')

            self.logger.info('Start of Archiving Old Result Files...')
            source = self.data_path + '_results/'
            if os.path.isdir(source):
                path = self.data_path + '_archive'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + '/results_' + str(date) + '_' + str(time)
                files = os.listdir(source)
                for file in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if file not in os.listdir(dest):
                        shutil.move(source + file, dest)
            self.logger.info('End of Archiving Old Result Files!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Archiving Old Rejected Files: %s' % e)
            raise e

    def move_processed_files(self):
        """
        * method: move_processed_files
        * description: To move processed files
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        try:
            self.logger.info('Start of Moving Processed Files...')
            for file in listdir(self.data_path):
                shutil.move(self.data_path + '/' + file, self.data_path + '_processed')
                self.logger.info('Moved the already processed file %s' % file)
            self.logger.info('End of Moving Processed Files!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Moving Processed Files: %s' % e)
            raise e

    def validate_trainset(self):
        """
        * method: validate_trainset
        * description: To validate the train data
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        try:
            self.logger.info('Start of Train Data Load, Validation, and Transformation...')
            self.archive_old_files()
            column_names, number_of_columns = self.values_from_schema('schema_train')
            self.validate_column_length(number_of_columns)
            self.validate_missing_values()
            self.replace_missing_values()

            self.dbOperation.create_table('training', 'training_raw_data_t', column_names)
            self.dbOperation.insert_data('training', 'training_raw_data_t')
            self.dbOperation.export_csv('training', 'training_raw_data_t')
            self.move_processed_files()
            self.logger.info('End of Train Data Load, Validation, and Transformation!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Train Data Load, Validation, and Transformation %s' % e)
            raise e

    def validate_predictset(self):
        """
        * method: validate_predictset
        * description: To validate the predict data
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     07-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None:
        """
        try:
            self.logger.info('Start of Predict Data Load, Validation, and Transformation...')
            self.archive_old_files()
            column_names, number_of_columns = self.values_from_schema('schema_predict')
            self.validate_column_length(number_of_columns)
            self.validate_missing_values()
            self.replace_missing_values()

            self.dbOperation.create_table('prediction', 'prediction_raw_data_t', column_names)
            self.dbOperation.insert_data('prediction', 'prediction_raw_data_t')
            self.dbOperation.export_csv('prediction', 'prediction_raw_data_t')
            self.move_processed_files()
            self.logger.info('End of Predict Data Load, Validation, and Transformation!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Predict Data Load, Validation, and Transformation %s' % e)
            raise e
