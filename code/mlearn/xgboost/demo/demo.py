#! /usr/bin/env python
# -*- coding:utf8 -*-

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from xgboost import plot_tree

# train data
train_data = pd.read_csv('D:\\liuli\\workspace\\quantitative\\code\\mlearn\\xgboost\\demo\\train.csv')
print(train_data.head())
print(train_data.info())
print(train_data.describe())
print(train_data.isnull().sum())
print(train_data.shape)

train_data.drop(['Alley', 'PoolQC', 'Fence', 'MiscFeature', 'Id', 'GarageYrBlt'], axis=1, inplace = True)

train_data['LotFrontage']=train_data['LotFrontage'].fillna(train_data['LotFrontage'].mode()[0])
train_data['BsmtCond']=train_data['BsmtCond'].fillna(train_data['BsmtCond'].mode()[0])
train_data['BsmtQual']=train_data['BsmtQual'].fillna(train_data['BsmtQual'].mode()[0])
train_data['FireplaceQu']=train_data['FireplaceQu'].fillna(train_data['FireplaceQu'].mode()[0])
train_data['GarageType']=train_data['GarageType'].fillna(train_data['GarageType'].mode()[0])
train_data['GarageFinish']=train_data['GarageFinish'].fillna(train_data['GarageFinish'].mode()[0])
train_data['GarageQual']=train_data['GarageQual'].fillna(train_data['GarageQual'].mode()[0])
train_data['GarageCond']=train_data['GarageCond'].fillna(train_data['GarageCond'].mode()[0])
print(train_data.shape)
print(train_data.isnull().sum())

import seaborn as sns
sns.heatmap(train_data.isnull(),yticklabels=False,cbar=False,cmap='coolwarm')

train_data['MasVnrType']=train_data['MasVnrType'].fillna(train_data['MasVnrType'].mode()[0])
train_data['MasVnrArea']=train_data['MasVnrArea'].fillna(train_data['MasVnrArea'].mode()[0])
train_data['BsmtExposure']=train_data['BsmtExposure'].fillna(train_data['BsmtExposure'].mode()[0])
train_data['BsmtFinType2']=train_data['BsmtFinType2'].fillna(train_data['BsmtFinType2'].mode()[0])

sns.heatmap(train_data.isnull(),yticklabels=False,cbar=False,cmap='YlGnBu')

train_data.dropna(inplace=True)
print(train_data.head())

#test data
test_data = pd.read_csv('D:\\liuli\\workspace\\quantitative\\code\\mlearn\\xgboost\\demo\\test.csv')
print(test_data.isnull().sum())
print(test_data.shape)

test_data.drop(['Alley', 'PoolQC', 'Fence', 'MiscFeature', 'Id', 'GarageYrBlt'], axis=1, inplace = True)

test_data['LotFrontage']=test_data['LotFrontage'].fillna(test_data['LotFrontage'].mode()[0])
test_data['BsmtCond']=test_data['BsmtCond'].fillna(test_data['BsmtCond'].mode()[0])
test_data['BsmtQual']=test_data['BsmtQual'].fillna(test_data['BsmtQual'].mode()[0])
test_data['FireplaceQu']=test_data['FireplaceQu'].fillna(test_data['FireplaceQu'].mode()[0])
test_data['GarageType']=test_data['GarageType'].fillna(test_data['GarageType'].mode()[0])
test_data['GarageFinish']=test_data['GarageFinish'].fillna(test_data['GarageFinish'].mode()[0])
test_data['GarageQual']=test_data['GarageQual'].fillna(test_data['GarageQual'].mode()[0])
test_data['GarageCond']=test_data['GarageCond'].fillna(test_data['GarageCond'].mode()[0])
test_data['MasVnrType']=test_data['MasVnrType'].fillna(test_data['MasVnrType'].mode()[0])
test_data['MasVnrArea']=test_data['MasVnrArea'].fillna(test_data['MasVnrArea'].mode()[0])
test_data['BsmtExposure']=test_data['BsmtExposure'].fillna(test_data['BsmtExposure'].mode()[0])
test_data['BsmtFinType2']=test_data['BsmtFinType2'].fillna(test_data['BsmtFinType2'].mode()[0])

print(test_data.loc[:, test_data.isnull().any()].head())

test_data['Utilities']=test_data['Utilities'].fillna(test_data['Utilities'].mode()[0])
test_data['Exterior1st']=test_data['Exterior1st'].fillna(test_data['Exterior1st'].mode()[0])
test_data['Exterior2nd']=test_data['Exterior2nd'].fillna(test_data['Exterior2nd'].mode()[0])
test_data['BsmtFinType1']=test_data['BsmtFinType1'].fillna(test_data['BsmtFinType1'].mode()[0])
test_data['BsmtFinSF1']=test_data['BsmtFinSF1'].fillna(test_data['BsmtFinSF1'].mean())
test_data['BsmtFinSF2']=test_data['BsmtFinSF2'].fillna(test_data['BsmtFinSF2'].mean())
test_data['BsmtUnfSF']=test_data['BsmtUnfSF'].fillna(test_data['BsmtUnfSF'].mean())
test_data['TotalBsmtSF']=test_data['TotalBsmtSF'].fillna(test_data['TotalBsmtSF'].mean())
test_data['BsmtFullBath']=test_data['BsmtFullBath'].fillna(test_data['BsmtFullBath'].mode()[0])
test_data['BsmtHalfBath']=test_data['BsmtHalfBath'].fillna(test_data['BsmtHalfBath'].mode()[0])
test_data['KitchenQual']=test_data['KitchenQual'].fillna(test_data['KitchenQual'].mode()[0])
test_data['Functional']=test_data['Functional'].fillna(test_data['Functional'].mode()[0])
test_data['GarageCars']=test_data['GarageCars'].fillna(test_data['GarageCars'].mean())
test_data['GarageArea']=test_data['GarageArea'].fillna(test_data['GarageArea'].mean())
test_data['SaleType']=test_data['SaleType'].fillna(test_data['SaleType'].mode()[0])

print(test_data.shape)

# Handling Categorical Features
columns=['MSZoning','Street','LotShape','LandContour','Utilities','LotConfig','LandSlope','Neighborhood',
         'Condition2','BldgType','Condition1','HouseStyle','SaleType',
        'SaleCondition','ExterCond',
         'ExterQual','Foundation','BsmtQual','BsmtCond','BsmtExposure','BsmtFinType1','BsmtFinType2',
        'RoofStyle','RoofMatl','Exterior1st','Exterior2nd','MasVnrType','Heating','HeatingQC',
         'CentralAir',
         'Electrical','KitchenQual','Functional',
         'FireplaceQu','GarageType','GarageFinish','GarageQual','GarageCond','PavedDrive']


def category_onehot_multcols(multcolumns):
    data_final = final_data
    i = 0
    for fields in multcolumns:
        print(fields)
        df1 = pd.get_dummies(final_data[fields], drop_first=True)
        final_data.drop([fields], axis=1, inplace=True)
        if i == 0:
            data_final = df1.copy()
        else:
            data_final = pd.concat([data_final, df1], axis=1)
        i = i + 1
    data_final = pd.concat([final_data, data_final], axis=1)
    return data_final

train_data2 = train_data.copy()

final_data=pd.concat([train_data,test_data],axis=0)
print(final_data['SalePrice'])
print(final_data.shape)

final_data=category_onehot_multcols(columns)
final_data =final_data.loc[:,~final_data.columns.duplicated()]

data_train=final_data.iloc[:1422,:]
data_test=final_data.iloc[1422:,:]
data_test.drop(['SalePrice'],axis=1,inplace=True)
X_train=data_train.drop(['SalePrice'],axis=1)
y_train=data_train['SalePrice']

import xgboost
regressor=xgboost.XGBRegressor(base_score=0.25, booster='gbtree', colsample_bylevel=1,
       colsample_bytree=1, gamma=0, learning_rate=0.1, max_delta_step=0,
       max_depth=2, min_child_weight=1, n_estimators=900,
       n_jobs=1, nthread=None, objective='reg:linear', random_state=0,
       reg_alpha=0, reg_lambda=1, scale_pos_weight=1, seed=None,
       silent=True, subsample=1)
regressor.fit(X_train,y_train)
y_pred=regressor.predict(data_test)
print(y_pred)

# 为了验证测试结果，划分训练集为新的训练集和测试集
data_train_new=data_train.iloc[:948,:]
data_test_new=data_train.iloc[948:,:]
y_data_test_new=data_test_new['SalePrice']
data_test_new.drop(['SalePrice'],axis=1,inplace=True)
X_train_new=data_train_new.drop(['SalePrice'],axis=1)
y_train_new=data_train_new['SalePrice']
model = regressor.fit(X_train_new, y_train_new)
plot_tree(model)
y_pred_new=regressor.predict(data_test_new)
print(y_pred_new)
print(y_data_test_new)
print(pearsonr(y_pred_new, y_data_test_new))

# data_train前20条记录抹0，看对训练的影响
data_train_new.iloc[:50, :] = np.nan
X_train_new=data_train_new.drop(['SalePrice'],axis=1)
y_train_new=data_train_new['SalePrice']
model = regressor.fit(X_train_new, y_train_new)
plot_tree(model)
y_pred_new=regressor.predict(data_test_new)
print(y_pred_new)
print(y_data_test_new)
print(pearsonr(y_pred_new, y_data_test_new))