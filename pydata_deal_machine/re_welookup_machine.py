import pandas as pd

class Welookup:
    def __init__(self,left,right,on = None,
                 left_col = None,right_col = None,need = None,
                 lable = None,drop_duplicates_keep = 'first',
                 combine = True,sortby = None):

        self.Data_orginal_left = left
        self.Data_orginal_right = right
        self.both_key = on

        self.left_key = left_col
        self.right_key = right_col
        self.needed_columns = need

        self.combine = combine
        self.lable = lable
        self.drop_duplicates_keep = drop_duplicates_keep
        self.sortby = sortby

    def left_key_deal(self,x):
            return x.split('$')


    def right_data_extract(self):
        right_key = self.right_key.split('$')
        if self.needed_columns:
            return self.Data_orginal_right
        else:
            right_get_columns = right_key + self.needed_columns
            return right_key,self.Data_orginal_right.loc[:,right_get_columns]

    def right_data_deal(self):
        right_key,right_orginal = self.right_data_extract()
        for sub,value in enumerate(right_key):




    def both_merge(self):




    def summary(self):
        if not self.both_key and self.needed_columns:
            result = pd.merge_ordered(self.Data_orginal_left,
                                      self.Data_orginal_right,on=self.both_key,how='left')
            return result

        if
