import pickle
import os
import shutil
from apps.core.logger import Logger


class FileOperation:
    """
    *****************************************************************************
    *
    * filename:       FileOperation.py
    * version:        1.0
    * author:         VIJAY-GADRE
    * creation date:  06-MAY-2023
    *
    * change history:
    *
    * who             when           version  change (include bug# if apply)
    * ----------      -----------    -------  ------------------------------
    * VIJAY-GADRE     06-MAY-2020    1.0      initial creation
    *
    *
    * description:    Class for file operations
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'FileOperation', mode)

    def save_model(self, model, file_name):
        """
        * method: save_model
        * description: To save the model file
        * return: File gets saved
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     06-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   model:
        *   file_name:
        """
        try:
            self.logger.info('Start of Save Models...')
            path = os.path.join('apps/models/', file_name)  # create separate directory for each cluster
            if os.path.isdir(path):
                shutil.rmtree('apps/models')
                os.makedirs(path)
            else:
                os.makedirs(path)

            with open(path + '/' + file_name + '.sav', 'wb') as f:
                pickle.dump(model, f)

            self.logger.info('Model File: ' + file_name + 'saved')
            self.logger.info('End of Save Models!!!')
            return 'success'

        except Exception as e:
            self.logger.exception('Exception raised while Save Models: %s' % str(e))
            raise Exception()

    def load_model(self, file_name):
        """
        * method: load_model
        * description: To load the model file
        * return: File gets loaded
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     06-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   file_name:
        """
        try:
            self.logger.info('Start of Load Model...')
            with open('apps/models/' + file_name + '/' + file_name + '.sav', 'rb') as f:
                self.logger.info('Model File ' + file_name + 'loaded')
                self.logger.info('End of Load Model!!!')
                return pickle.load(f)

        except Exception as e:
            self.logger.exception('Exception raised while Loading Model: %s' % str(e))
            raise Exception()

    def correct_model(self, cluster_number):
        """
        * method: correct_model
        * description: To find the best model
        * return: The Model file
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     06-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   cluster_number:
        """
        try:
            self.logger.info('Start of finding correct model...')
            self.cluster_number = cluster_number
            self.folder_name = 'apps/models'
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)

            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number)) != -1):
                        self.model_name = self.file
                except:
                    continue

            self.model_name = self.model_name.split('.')[0]
            self.logger.info('End of finding correct model!!!')
            return self.model_name

        except Exception as e:
            self.logger.info('Exception raised while finding correct model' + str(e))
            raise Exception()
