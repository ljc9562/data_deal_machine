# Data_deal_machine

### 數據清洗引擎(基於pandas庫)
 - 读取合并(weinput_machine.py) ：同文件夹多个数据合并，同excel多工作簿合并
 - 文件匹配(welookup_machine.py)：类似excel公式 Vlookup 你懂的
 - 标签引擎(wetagging_mechine.py)：用于处理外置的标签配置文件
 - 文件输出(weoutput_mechine.py)：用于输出dataframe到数据库
 
### 其他工具
- 日期处理(wedtclean.py)：用于清洗日期数据,自动对纯日期和(带文本日期或纯文本)分类
---
### 引擎使用去中心化的方法處理數據
- 推荐使用python自带的jutyper notebook进行脚本编写
- 日常处理流程:
1. 先读取数据(参见pandas.read方法,左右连接的key值设置文本型)如果需要同文件夹合并可以使用weinput的方法
2. 合并数据 使用welookup的方法对数据进行合并(函数详情help(welookup))
3. 对数据打标签,使用外置标签配置文件的方法,标签的方法请看下文 or 参见(help(wetagging))
4. 如需要合并数据可以使用pandas.concat()方法.(更多操作可以查询pandas文档)
5. 文件输出.如需要输出excel和csv 只需要对结果使用pandas.to_excel或to_csv*方法,特殊一点需要输出到sql数据库,我提供了一个weoutput的方法,只要输入dataframe和您独有的sql信息即可输出(数据库登陆基于sqlalchemy,登陆方法可以参见其create_engine的方法)

### 標簽配置文件設置

---
- ###标签文件都必须的通用标注
1. 设置的路径 .\config\Tagcode_home (路径可以任意设置)
2. 标签文件类型分为 脚本处理(script) 或 简易方法处理包括简易正则,统称(re) 
文件的类型需要在文件的头部声明 format： #category:script  或者   #category:re ,该声明必须在首行
3. 标签描述,该标签用途,适用范围,格式规约等 format： #markdown:简介；范围；规约
4. 标签版本情况,  format：#version:[年份+季节] [时间 yyyymmdd]  ex: #version:2018冬季 20181001
5. 一些常用的命令可以先写在辅助里面
---
- ####标签文件类型为re 的标签配置
1. new_column_name(新列名) 如 new_column_name = '区域'
2. columns_type(列的字段数据类型) columns_type = 'str'
3. rule(标签的规则) 独立条件的格式写法 独立条件 = "标签值->(frame['列名'][逻辑符/逻辑函数/正则][value])"
多个条件满足写法 例如:"标签值->((独立条件)|(独立条件))&(独立条件)"--多个标签写法 例如["标签","标签"]

4. else_value rule中都没有情况的填充值 例子:
\
\
\#未知的情况  便签为  未知地址\
else_value = "未知地址"
\
\
\#未知的情况  标签为  '未知地址_'+列['name']值\
else_value = "'未知地址_' + frame['name']"\
\
完整代码块:
>
    #category:re
    #author:jc
    #cn_name:
    #markdowm:作用
    #版本:2018冬-20181001
    #辅助 包含[str.contains] ps(如果需要正则,增加参数regex = True)
    #逻辑符号 1.和["&"]  2.且["|"]  3.非["~"] 
    #若输出是按列操作 需要加入frame[...]
    #re表达式格式: ""
    
    new_column_name = '区域'
    columns_type = 'str'
    
    
    #format
    rule = 
    [
    #例子1:开头为周的地区标签为 周(该处使用了正则,所以regex = True)
    "周->(frame['name'].str.contains('^周',regex = True))",
    
    #例子2:citycode列 大于700的数据 标签为 大于700哦
    "大于700哦->(frame['citycode']>=700)"
    
    #例子3:开头为周的地区标签为 周 且 citycode列 大于700的数据 标签为 开头周和大于700  
    "开头周和大于700->(frame['name'].str.contains('^周',regex = True))&(frame['citycode']>=700)"
    
    #例子4:同时值多个包含情况 例子 name字段包含 市或地区 的 便签为 地区
    "地区->(frame['name'].str.contains('市|地区'))
    
    #例子5:输出的标签值是其他列的值或其他列的替换情况等 标记为name列的值 + '_地区' 这个后缀(也可以'地区_'+frame['name'])
    "frame['name']+'_地区'->(frame['name'].str.contains('市|地区'))
    
    #例子6:输出的标签值是列的替换值(详情pd.Dataframe.replace())
    #精确替换 把对应字段中的a值替换为b
    "frame['name'].replace("a","b")->(frame['name'].str.contains('市|地区'))
    #模糊替换 把对应字段中的a值替换为b
    "frame['name'].replace("a","b",regex = True)->(frame['name'].str.contains('市|地区'))
    ]
    
    else_value = "未知"



---
- ####标签文件类型为**script** 的标签配置
1. script方法原理:该方法是被包裹在函数中,传入的是一个完整的frame
2. 编写自定义的任何处理数据的脚本即可,可以导入任何外部的库
3. 最后return的数据都需要赋值给frame才能完成完整的过程,因为源码中处理这一块的函数是(return frame)\
\
代码块:
>
    #category:script
    #author:jc
    #cn_name:
    #markdowm:作用
    #版本:2018冬-20181001
    #传入frame 可以做任何外部库的操作 以下仅仅是简单操作
    new_column_name = '地区情况'
    get_columns = ['name','citycode']
    
    def aa(data):
        city = data[0]
        citycode = data[1]
        if '地区' in city:
            return '地区'
        elif citycode >700:
            return '大于700'
        else:
            return '未知'
    
    frame[new_column_name] = frame[get_columns].apply(aa,axis = 1)

