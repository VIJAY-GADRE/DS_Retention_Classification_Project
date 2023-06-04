from apps.core.logger import Logger
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import r2_score, roc_auc_score, accuracy_score


class ModelTuner:
    """
    *****************************************************************************
    *
    * filename:       model_tuner.py
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
    * description:    Class to tune and select the best model
    *
    ****************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'ModelTuner', mode)
        self.rfc = RandomForestClassifier()
        self.xgb = XGBClassifier(objective='binary:logistic')

    def best_params_randomForest(self, train_x, train_y):
        """
        * method: best_params_randomForest
        * description: To get the parameters for Random Forest Classifier which gives the best Accuracy
        * return: a model with the best parameters
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   train_x
        *   train_y
        """
        try:
            self.logger.info('Start of finding the Best Parameters for Random Forest Classifier...')
            self.param_grid = {'n_estimators': [10, 50, 100, 150],
                               'criterion': ['gini', 'entropy'],
                               'max_depth': range(2, 4, 1),
                               'max_features': ['auto', 'log2']}
            self.grid = GridSearchCV(estimator=self.rfc, param_grid=self.param_grid, cv=5)
            self.grid.fit(train_x, train_y)

            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.n_estimators = self.grid.best_params_['n_estimators']

            self.rfc = RandomForestClassifier(n_estimators=self.n_estimators,
                                              criterion=self.criterion,
                                              max_depth=self.max_depth,
                                              max_features=self.max_features)
            self.rfc.fit(train_x, train_y)
            self.logger.info('Random Forest Best Parameters:' + str(self.grid.best_params_))
            self.logger.info('End of finding the Best Parameters for Random Forest Classifier!!!')

            return self.rfc

        except Exception as e:
            self.logger.exception('Exception raised while finding the Best Parameters for Random Forest Classifier: %s' + str(e))
            raise Exception()

    def best_params_xgboost(self, train_x, train_y):
        """
        * method: best_params_xgboost
        * description: To get the parameters for XGBoost Classifier which gives the best Accuracy
        * return: a model with the best parameters
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   train_x
        *   train_y
        """
        try:
            self.logger.info('Start of finding the Best Parameters for XGBoost Classifier...')
            self.param_grid_xgb = {'learning_rate': [0.5, 0.1, 0.01, 0.001],
                               'n_estimators': [10, 50, 100, 150],
                               'max_depth': range(2, 4, 1)}
            self.grid = GridSearchCV(XGBClassifier(objective='binary:logistic'), self.param_grid_xgb, cv=5)
            self.grid.fit(train_x, train_y)

            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            self.xgb = XGBClassifier(objective='binary:logistic',
                                     learning_rate=self.learning_rate,
                                     n_estimators=self.n_estimators,
                                     max_depth=self.max_depth)
            self.xgb.fit(train_x, train_y)
            self.logger.info('Random Forest Best Parameters:' + str(self.grid.best_params_))
            self.logger.info('End of finding the Best Parameters for XGBoost Classifier!!!')

            return self.xgb

        except Exception as e:
            self.logger.exception('Exception raised while finding the Best Parameters for XGBoost Classifier: %s' + str(e))
            raise Exception()

    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
        * method: get_best_model
        * description: To get the best model which gives the best Accuracy
        * return: a model with the best parameters
        *
        * who             when           version  change (include bug# if apply)
        * ----------      -----------    -------  ------------------------------
        * VIJAY-GADRE     09-MAY-2020    1.0      initial creation
        *
        * Parameters
        *   train_x
        *   train_y
        *   test_x
        *   test_y
        """
        try:
            self.logger.info('Start of finding the Best Model...')
            self.xgboost = self.best_params_xgboost(train_x, train_y)
            self.prediction_xgboost = self.xgboost.predict(test_x)

            if len(test_y.unique()) == 1:
                self.xgboost_score = accuracy_score(test_y, self.prediction_xgboost)
                self.logger.info('Accuracy of XGBoost:' + str(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(test_y, self.prediction_xgboost)
                self.logger.info('AUC of XGBoost:' + str(self.xgboost_score))

            self.random_forest = self.best_params_randomForest(train_x, train_y)
            self.prediction_random_forest = self.random_forest.predict(test_x)

            if len(test_y.unique()) == 1:
                self.random_forest_score = accuracy_score(test_y, self.prediction_random_forest)
                self.logger.info('Accuracy of Random Forest:' + str(self.random_forest_score))

            else:
                self.random_forest_score = roc_auc_score(test_y, self.prediction_random_forest)
                self.logger.info('AUC of Random Forest:' + str(self.random_forest_score))

            self.logger.info('End of finding the Best Model!!!')

            if (self.random_forest_score < self.xgboost_score):
                return 'XGBoost', self.xgboost
            else:
                return 'RandomForest', self.random_forest

        except Exception as e:
            self.logger.exception('Exception raised while finding the best model %s:' + str(e))
            raise Exception()