# Data_deal_machine

### 数据清洗引擎(基于pandas库)
 - 读取合并(weinput_machine.py) ：同文件夹多个数据合并，同excel多工作簿合并
 - 文件匹配(welookup_machine.py)：类似excel公式 Vlookup 你懂的
 - 标签引擎(wetagging_mechine.py)：用于处理外置的标签配置文件
 - 文件输出(weoutput_mechine.py)：用于输出dataframe到数据库
 
### 其他工具
- 日期处理(wedtclean.py)：用于清洗日期数据,自动对纯日期和(带文本日期或纯文本)分类
---
### 引擎使用去中心化的办法处理数据
- 推荐使用python自带的jutyper notebook进行脚本编写
- 日常处理演绎例子(基于):
1. 先读取数据(参见pandas.read方法,左右连接的key值设置文本型)如果需要同文件夹合并可以使用weinput的方法
2. 合并数据 使用welookup的方法对数据进行合并(函数详情help(welookup))
3. 对数据打标签()