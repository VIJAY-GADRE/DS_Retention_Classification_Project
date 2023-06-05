from apps.core.logger import Logger
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from sklearn.model_selection import train_test_split
from apps.core.file_operation import FileOperation
from apps.tuning.model_tuner import ModelTuner
from apps.ingestion.load_validate import LoadValidate
from apps.preprocess.preprocessor import Preprocessor


class KMeansCluster:
    """
    *****************************************************************************
    *
    * filename:       cluster.py
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
    * description:    Class to cluster the dataset
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'KMeansCluster', 'training')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'training')

    def elbow_plot(self, data):
        """
        * method: elbow_plot
        * description: To decide the optimum number of clusters by creating a plot
        * return: a picture saved into the directory
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     10-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data
        """
        wcss = []
        try:
            self.logger.info('Start of Elbow Plotting...')
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)
            plt.title('Elbow Method')
            plt.xlabel('Number of Clusters')
            plt.ylabel('WCSS')
            plt.savefig('apps/models/kmeans_elbow.png')

            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger.info('The optimum number of clusters is:' + str(self.kn.knee))
            self.logger.info('End of Elbow Plotting!!!')
            return self.kn.knee

        except Exception as e:
            self.logger.exception('Exception raised while elbow plotting: %s' + str(e))
            raise Exception()

    def create_clusters(self, data, number_of_clusters):
        """
        * method: create_clusters
        * description: To create clusters
        * return: a dataframe with cluster column
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     10-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   data
        *   number_of_clusters
        """
        self.data = data
        try:
            self.logger.info('Start of Creating Clusters...')
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            self.y_kmeans = self.kmeans.fit_predict(data)
            self.saveModel = self.fileOperation.save_model(self.kmeans, 'KMeans')
            self.data['Cluster'] = self.y_kmeans
            self.logger.info('Successfully created ' + str(self.kn.knee) + ' clusters.')
            self.logger.info('End of Creating Clusters!!!')
            return self.data

        except Exception as e:
            self.logger.exception('Exception raised while Creating Clusters: %s' + str(e))
            raise Exception
