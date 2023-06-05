import pandas as pd
from apps.core.logger import Logger
from apps.ingestion.load_validate import LoadValidate
from apps.preprocess.preprocessor import Preprocessor
from apps.core.file_operation import FileOperation


class PredictModel:
    """
    *****************************************************************************
    *
    * filename:       predict_model.py
    * version:        1.0
    * author:         VIJAY-GADRE
    * creation date:  09-MAY-2023
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class to predict the results
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'PredictModel', 'prediction')
        self.loadValidate = LoadValidate(self.run_id, self.data_path, 'prediction')
        self.preProcess = Preprocessor(self.run_id, self.data_path, 'prediction')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'prediction')

    def batch_predict_from_model(self):
        """
        * method: batch_predict_from_model
        * description: To predict the results
        * return: a pandas dataframe
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None
        """
        try:
            self.logger.info('Start of Batch Prediction from Model...')
            self.logger.info('run_id:' + str(self.run_id))
            self.loadValidate.validate_predictset()  # validation and transformation
            self.X = self.preProcess.preprocess_predictset()  # preprocessing activity
            kmeans = self.fileOperation.load_model('KMeans')
            clusters = kmeans.predict(self.X.drop(['empid'], axis=1))
            self.X['clusters'] = clusters
            clusters = self.X['clusters'].unique()

            y_predicted = []
            for i in clusters:
                self.logger.info('Clusters Loop Started...')
                cluster_data = self.X[self.X['clusters'] == i]
                cluster_data_new = cluster_data.drop(['empid', 'clusters'], axis=1)
                model_name = self.fileOperation.correct_model(i)
                model = self.fileOperation.load_model(model_name)
                y_predicted = model.predict(cluster_data_new)
                result = pd.DataFrame({'EmpId': cluster_data['empid'],
                                       'Prediction': y_predicted})
                result.to_csv(self.data_path + '_results/' + 'Predictions.csv', header=True, mode='a+', index=False)
            self.logger.info('End of Batch Prediction from Model!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Batch Prediction from Model: %s', str(e))
            raise Exception()

    def single_predict_from_model(self, data):
        """
        * method: single_predict_from_model
        * description: To predict the results
        * return: a pandas dataframe
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data:
        """
        try:
            self.logger.info('Start of Single Prediction from Model...')
            self.logger.info('run_id:' + str(self.run_id))
            self.X = self.preProcess.preprocess_predict(data)  # preprocessing activity
            kmeans = self.fileOperation.load_model('KMeans')
            clusters = kmeans.predict(self.X.drop(['empid'], axis=1))
            self.X['clusters'] = clusters
            clusters = self.X['clusters'].unique()

            y_predicted = []
            for i in clusters:
                self.logger.info('Clusters Loop Started...')
                cluster_data = self.X[self.X['clusters'] == i]
                cluster_data_new = cluster_data.drop(['empid', 'clusters'], axis=1)
                model_name = self.fileOperation.correct_model(i)
                model = self.fileOperation.load_model(model_name)

                self.logger.info('Shape of Data:' + str(cluster_data_new.shape))
                self.logger.info('Info of Data:' + str(cluster_data_new.info()))
                y_predicted = model.predict(cluster_data_new)
                self.logger.info('Output:' + str(y_predicted))
                self.logger.info('End of Single Prediction from Model!!!')
                return int(y_predicted[0])

        except Exception as e:
            self.logger.exception('Exception raised while Single Prediction from Model: %s', str(e))
            raise Exception()
