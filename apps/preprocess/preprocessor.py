import pandas as pd
import numpy as np
import json
from sklearn.impute import KNNImputer
from apps.core.logger import Logger


class Preprocessor:
    """
    *****************************************************************************
    *
    * filename:       preprocessor.py
    * version:        1.0
    * author:         VIJAY-GADRE
    * creation date:  08-MAY-2023
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class to preprocess training and predict dataset
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'Preprocessor', mode)

    def get_data(self):
        """
        * method: get_data
        * description: To read data file
        * return: a pandas dataframe
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None
        """
        try:
            self.logger.info('Start of reading dataset...')
            self.data = pd.read_csv(self.data_path + '_validation/InputFile.csv')
            self.logger.info('End of reading dataset!!!')
            return self.data

        except Exception as e:
            self.logger.exception('Exception raised while reading the dataset: %s' + str(e))
            raise Exception()

    def drop_columns(self, data, columns):
        """
        * method: drop_columns
        * description: To drop the irrelevant columns
        * return: a pandas dataframe after removing the irrelevant columns
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        *   columns:
        """
        self.data = data
        self.columns = columns
        try:
            self.logger.info('Start of Dropping the Irrelevant Columns...')
            self.useful_data = self.data.drop(labels=self.columns, axis=1)
            self.logger.info('End of Dropping the Irrelevant Columns!!!')
            return self.useful_data

        except Exception as e:
            self.logger.exception('Exception raised while Dropping Irrelevant Columns: %s' + str(e))
            raise Exception()

    def is_null_present(self, data):
        """
        * method: is_null_present
        * description: To check null values
        * return: a boolean value. (True - Null values are present in the dataframe, Else False)
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        self.null_present = False
        try:
            self.logger.info('Start of finding the missing values...')
            self.null_counts = data.isna().sum()
            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break
            if self.null_present:
                df_with_null = pd.DataFrame()
                df_with_null['columns'] = data.columns
                df_with_null['missing_values_count'] = np.asarray(data.isna().sum())
                df_with_null.to_csv(self.data_path + '_validation/' + 'null_values.csv')
            self.logger.info('End of finding the missing values!!!')
            return self.null_present

        except Exception as e:
            self.logger.exception('Exception raised while finding missing values: %s' + str(e))
            raise Exception()

    def impute_missing_values(self, data):
        """
        * method: impute_missing_values
        * description: To impute missing values
        * return: a pandas dataframe after imputing the missing values
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        self.data = data
        try:
            self.logger.info('Start of Imputing Missing values...')
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            self.new_array = imputer.fit_transform(self.data)
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger.info('End of Imputing Missing values!!!')
            return self.new_data

        except Exception as e:
            self.logger.exception('Exception raised while imputing missing values: %s' + str(e))
            raise Exception()

    def feature_encoding(self, data):
        """
        * method: feature_encoding
        * description: To encode the categorical features
        * return: a pandas dataframe after encoding the categorical features
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        try:
            self.logger.info('Start of Feature Encoding...')
            self.new_data = data.select_dtypes(include=['object']).copy()
            for col in self.new_data.columns:
                self.new_data = pd.get_dummies(self.new_data, columns=[col], prefix=[col], drop_first=True)
            self.logger.info('End of Feature Encoding!!!')
            return self.new_data

        except Exception as e:
            self.logger.exception('Exception raised while feature encoding: %s' + str(e))
            raise Exception()

    def split_features_label(self, data, label_name):
        """
        * method: split_features_label
        * description: To separate features and label
        * return: Features and Label
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        self.data = data
        try:
            self.logger.info('Start of Splitting features and label...')
            self.X = self.data.drop(labels=label_name, axis=1)
            self.y = self.data[label_name]
            self.logger.info('End of Splitting features and label!!!')

        except Exception as e:
            self.logger.exception('Exception raised while splitting features and label: %s' + str(e))
            raise Exception()

    def final_predictset(self, data):
        """
        * method: final_predictset
        * description: To build final predict set by adding an encoded column with value = 0
        * return: a pandas dataframe after building a final predict set
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        try:
            self.logger.info('Start of Building Final Predict Set...')
            with open('apps/database/columns.json', 'r') as f:
                data_columns = json.load(f)['data_columns']
                f.close()
            df = pd.DataFrame(data=None, columns=data_columns)
            df_new = pd.concat([df, data], ignore_index=True, sort=False)
            data_new = df_new.fillna(0)
            self.logger.info('End of Building Final Predict Set!!!')
            return data_new

        except ValueError:
            self.logger.exception('ValueError raised while Building Final Predict Set')
            raise ValueError
        except KeyError:
            self.logger.exception('KeyError raised while Building Final Predict Set')
            raise KeyError
        except Exception as e:
            self.logger.exception('Exception raised while Building Final Predict Set: %s' % e)
            raise e

    def preprocess_trainset(self):
        """
        * method: preprocess_trainset
        * description: To preprocess the training dataset
        * return: Preprocessed Features and Label
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None
        """
        try:
            self.logger.info('Start of Preprocessing Training Dataset...')
            data = self.get_data()
            data = self.drop_columns(data, ['empid'])
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            data = self.drop_columns(data, ['salary'])
            if self.is_null_present(data):
                data = self.impute_missing_values(data)
            self.X, self.y = self.split_features_label(data, label_name='left')
            self.logger.info('End of Preprocessing Training Dataset!!!')
            return self.X, self.y

        except Exception as e:
            self.logger.exception('Exception raised while Preprocessing Training Dataset: %s' + str(e))
            raise Exception()

    def preprocess_predictset(self):
        """
        * method: preprocess_predictset
        * description: To preprocess the testing dataset
        * return: Preprocessed Features
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None
        """
        try:
            self.logger.info('Start of Preprocessing Predicting Dataset...')
            data = self.get_data()
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            data = self.drop_columns(data, ['salary'])
            if self.is_null_present(data):
                data = self.impute_missing_values(data)
            data = self.final_predictset(data)
            self.logger.info('End of Preprocessing Predicting Dataset!!!')
            return data

        except Exception as e:
            self.logger.exception('Exception raised while Preprocessing Predicting Dataset: %s' + str(e))
            raise Exception()

    def preprocess_predict(self, data):
        """
        * method: preprocess_predict
        * description: To preprocess the prediction dataset
        * return: Preprocessed Predict Dataset
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     08-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        try:
            self.logger.info('Start of Preprocessing Predict Dataset...')
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis=1)
            data = self.drop_columns(data, ['salary'])
            if self.is_null_present(data):
                data = self.impute_missing_values(data)
            data = self.final_predictset(data)
            self.logger.info('End of Preprocessing Predict Dataset!!!')
            return data

        except Exception as e:
            self.logger.exception('Exception raised while Preprocessing Predict Dataset: %s' + str(e))
            raise Exception()
