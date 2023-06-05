import json
from apps.core.logger import Logger
from apps.core.file_operation import FileOperation
from apps.tuning.model_tuner import ModelTuner
from apps.ingestion.load_validate import LoadValidate
from apps.preprocess.preprocessor import Preprocessor
from apps.tuning.cluster import KMeansCluster
from sklearn.model_selection import train_test_split


class TrainModel:
    """
    *****************************************************************************
    *
    * filename:       train_model.py
    * version:        1.0
    * author:         VIJAY-GADRE
    * creation date:  10-MAY-2023
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     10-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class to train the models
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'TrainModel', 'training')
        self.loadValidate = LoadValidate(self.run_id, self.data_path, 'training')
        self.preProcess = Preprocessor(self.run_id, self.data_path, 'training')
        self.modelTuner = ModelTuner(self.run_id, self.data_path, 'training')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'training')
        self.cluster = KMeansCluster(self.run_id, self.data_path)

    def training_model(self):
        """
        * method: training_model
        * description: To train the model
        * return: None
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     10-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   None
        """
        try:
            self.logger.info('Start of Training the Model...')
            self.logger.info('Run_id:' + str(self.run_id))
            self.loadValidate.validate_trainset()  # Load, Validation, and Transform
            self.X, self.y = self.preProcess.preprocess_trainset()  # PreProcessing activities
            columns = {"data_columns": [col for col in self.X.columns]}
            with open('apps/database/columns.json', 'w') as f:
                f.write(json.dumps(columns))

            number_of_clusters = self.cluster.elbow_plot(self.X)  # Create Clusters
            self.X = self.cluster.create_clusters(self.X, number_of_clusters)  # Divide the dataset into clusters
            self.X['Labels'] = self.y  # Cluster Assignment
            list_of_clusters = self.X['Cluster'].unique()  # Unique Clusters from the dataset

            # Parsing all the clusters and looking for the best ML Algorithm to fit on individual cluster
            for i in list_of_clusters:
                cluster_data = self.X[self.X['Cluster'] == i]
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=0.2,
                                                                    random_state=42)
                best_model_name, best_model = self.modelTuner.get_best_model(x_train, x_test, y_train, y_test)
                save_model = self.fileOperation.save_model(best_model, best_model_name + str(i))

            self.logger.info('End of Training the Model!!!')

        except Exception as e:
            self.logger.exception('Exception raised while Training the Model: %s' + str(e))
            raise Exception()
