import pandas as pd


class Welookup:
    def __init__(self, left, right, on=None,
                 left_col=None, right_col=None, need=None,
                 lable=None, drop_duplicates_keep='first',
                 combine=True, sortby=None):

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

    def left_deal(self):
        return self.left_key.split('$'), self.Data_orginal_left

    def right_data_extract(self):
        right_key = self.right_key.split('$')

        if not self.needed_columns:
            right_need_columns = self.Data_orginal_right.columns.tolist()
            for key in right_key:
                right_need_columns.remove(key)
            return right_key, right_need_columns, self.Data_orginal_right
        else:
            right_need_columns = self.needed_columns.split('$')
            right_get_columns = right_key + right_need_columns
            return right_key, right_need_columns, self.Data_orginal_right.loc[:,
                                                                              right_get_columns]

    def right_data_deal(self):
        right_key, right_need_columns, right_orginal = self.right_data_extract()

        if len(right_key) == 1:
            signal_right_key = right_key[0]
            right_orginal.rename(
                columns={
                    signal_right_key: 'key_right'},
                inplace=True)
            right_orginal['key_right'] = right_orginal['key_right'].astype(str)
            return right_key, right_need_columns, right_orginal
        else:
            # 获取第一个Dataframe 方便合并
            first_right_key = right_key[0]
            right_orginal_result = right_orginal.loc[:, [
                first_right_key] + right_need_columns]
            right_orginal_result.rename(
                columns={first_right_key: 'key_right'}, inplace=True)

            # integrate the key with need columns when the count of key more than one
            for AnotherKey in range(1, len(right_key)):
                right_orginal_result_s = right_orginal.loc[:, [
                    right_key[AnotherKey]] + right_need_columns]
                right_orginal_result_s.rename(
                    columns={right_key[AnotherKey]: 'key_right'}, inplace=True)
                right_orginal_result = pd.concat(
                    [right_orginal_result, right_orginal_result_s], axis=0, ignore_index=True)

            #tranlate the type of right key
            right_orginal_result['key_right'] = right_orginal_result['key_right'].astype(
                str)
            #drop duplicates by key_right
            if self.sortby:
                right_orginal_result = right_orginal_result.sort_values(self.sortby).drop_duplicates(['key_right'],keep=self.drop_duplicates_keep)
            else:
                right_orginal_result = right_orginal_result.drop_duplicates(['key_right'],
                                                                            keep=self.drop_duplicates_keep)
            # Eliminate null or ""
            right_orginal_result = right_orginal_result[(
                (right_orginal_result.key_right != "")) & (right_orginal_result.key_right.notnull())]
            return right_key, right_need_columns, right_orginal_result

    def both_merge(self):
        left_keys, left_orginal_result = self.left_deal()
        right_keys, right_need_columns, right_orginal_result = self.right_data_deal()
        for left_key in left_keys:
            left_orginal_result = pd.merge(
                left_orginal_result,
                right_orginal_result,
                left_on=left_key,
                right_on='key_right',
                how='left')
        left_orginal_result = left_orginal_result.fillna("")
        return left_keys, right_need_columns, left_orginal_result

    def Integration_function(self, columns):
        self.columns = columns
        result = ""
        for column in range(len(self.columns)):
            if self.columns[column] != "":
                result = self.columns[column]
            else:
                pass
        return result

    def MergeResult_integration(self):
        left_keys, right_need_columns, result = self.both_merge()
        integration_columns_all = result.columns.tolist()[len(result.columns.tolist()) -
                                                   len(left_keys) *
                                                   (len(right_need_columns) + 1):]
        for right_need_column in right_need_columns:
            integration_col = [col for col in integration_columns_all if col.startswith(f'{right_need_column}')]
            if self.lable:
                result[f'{right_need_column}_{self.lable}'] = result[integration_col].apply(self.Integration_function,axis=1)
            else:
                result[right_need_column] = result[integration_col].apply(self.Integration_function,axis=1)

        for del_col in integration_columns_all:
            del result[del_col]

        return result


    def summary(self):
        if self.both_key:
            result = pd.merge(
                self.Data_orginal_left,
                self.Data_orginal_right,
                on=self.both_key,
                how='left')
            return result

        elif self.combine == False:
            *columns_info,result = self.both_merge()
            return result

        elif self.combine :
            result = self.MergeResult_integration()
            return result





if __name__ == '__main__':
    aim = pd.read_excel(
        r"C:\Users\85442\Desktop\20181102余量\测试A多.xlsx",
        converters={
            'loveId': str,
            'loveId2': str},
        keep_default_na=False)
    match = pd.read_excel(
        r"C:\Users\85442\Desktop\20181102余量\测试B短.xlsx",
        converters={
            'loveId': str,
            'loveId2': str},
        keep_default_na=False)
    a = Welookup(aim, match, left_col='loveId2$loveId',
                 right_col='loveId$loveId2',need='未通次数',sortby=['key_right','未通次数']).summary()
    a.to_excel(r'C:\Users\85442\Desktop\测试结果3.xlsx', index=False)