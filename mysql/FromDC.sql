################################################################################
## 用于建立 FromDC 的数据表。
## 包括：
## 1. daily
## 2. minute
################################################################################

CREATE DATABASE `FromDC` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

################################################################################～～～～～～～～～～～～
## FromDC.daily                                                               ## FromDC.daily
################################################################################～～～～～～～～～～～～
CREATE TABLE  FromDC.daily(
    TradingDay       DATE             NOT NULL,            ## 交易日期
    Sector           CHAR(20)         NOT NULL,            ## 日期属性: 
    #                                                      ## 1. 只含日盘: Sector = 'day'
    #                                                      ## 2. 只含夜盘: Sector = 'nights'
    #                                                      ## 3. 全天，包含日盘、夜盘: Sector = 'allday'
    InstrumentID     CHAR(30)         NOT NULL,            ## 合约名称
    #------------------------------------------------------
    OpenPrice        DECIMAL(15,5)          NULL,          ## 开盘价
    HighPrice        DECIMAL(15,5)          NULL,          ## 最高价
    LowPrice         DECIMAL(15,5)          NULL,          ## 最低价
    ClosePrice       DECIMAL(15,5)          NULL,          ## 收盘价
    #-----------------------------------------------------
    Volume           INT UNSIGNED           NULL,          ## 成交量
    Turnover         DECIMAL(30,3)          NULL,          ## 成交额
    #-----------------------------------------------------
    OpenOpenInterest  INT UNSIGNED          NULL,          ## 当日的开仓的开盘量
    HighOpenInterest  INT UNSIGNED          NULL,          ## 当日的开仓的最高量
    LowOpenInterest   INT UNSIGNED          NULL,          ## 当日的开仓的最低量
    CloseOpenInterest INT UNSIGNED          NULL,          ## 当日的开仓的收盘量，即 position
    #-----------------------------------------------------
    UpperLimitPrice  DECIMAL(15,5)          NULL,          ## 当日的有效最高报价
    LowerLimitPrice  DECIMAL(15,5)          NULL,          ## 当日的有效最低报价
    SettlementPrice  DECIMAL(15,5)          NULL,          ## 当日交易所公布的结算价
    #-----------------------------------------------------
    PRIMARY KEY (TradingDay, Sector, InstrumentID)         ## 主键唯一，重复不可输入
    );

##----------- INDEX --------------------------------------------------------- ##
CREATE INDEX index_daily
ON FromDC.daily
(TradingDay, Sector, InstrumentID);  
## -------------------------------------------------------------------------- ## 

##----------- PARTITIONS ---------------------------------------------------- ##
ALTER TABLE FromDC.daily
    PARTITION BY RANGE( TO_DAYS(TradingDay) )(
    #---------------------------------------------------------------------------
    PARTITION p_2010_01 VALUES LESS THAN (TO_DAYS('2010-02-01')),
    PARTITION p_2010_02 VALUES LESS THAN (TO_DAYS('2010-03-01')),
    PARTITION p_2010_03 VALUES LESS THAN (TO_DAYS('2010-04-01')),
    PARTITION p_2010_04 VALUES LESS THAN (TO_DAYS('2010-05-01')),
    PARTITION p_2010_05 VALUES LESS THAN (TO_DAYS('2010-06-01')),
    PARTITION p_2010_06 VALUES LESS THAN (TO_DAYS('2010-07-01')),
    PARTITION p_2010_07 VALUES LESS THAN (TO_DAYS('2010-08-01')),
    PARTITION p_2010_08 VALUES LESS THAN (TO_DAYS('2010-09-01')),
    PARTITION p_2010_09 VALUES LESS THAN (TO_DAYS('2010-10-01')),
    PARTITION p_2010_10 VALUES LESS THAN (TO_DAYS('2010-11-01')),
    PARTITION p_2010_11 VALUES LESS THAN (TO_DAYS('2010-12-01')),
    PARTITION p_2010_12 VALUES LESS THAN (TO_DAYS('2011-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2011_01 VALUES LESS THAN (TO_DAYS('2011-02-01')),
    PARTITION p_2011_02 VALUES LESS THAN (TO_DAYS('2011-03-01')),
    PARTITION p_2011_03 VALUES LESS THAN (TO_DAYS('2011-04-01')),
    PARTITION p_2011_04 VALUES LESS THAN (TO_DAYS('2011-05-01')),
    PARTITION p_2011_05 VALUES LESS THAN (TO_DAYS('2011-06-01')),
    PARTITION p_2011_06 VALUES LESS THAN (TO_DAYS('2011-07-01')),
    PARTITION p_2011_07 VALUES LESS THAN (TO_DAYS('2011-08-01')),
    PARTITION p_2011_08 VALUES LESS THAN (TO_DAYS('2011-09-01')),
    PARTITION p_2011_09 VALUES LESS THAN (TO_DAYS('2011-10-01')),
    PARTITION p_2011_10 VALUES LESS THAN (TO_DAYS('2011-11-01')),
    PARTITION p_2011_11 VALUES LESS THAN (TO_DAYS('2011-12-01')),
    PARTITION p_2011_12 VALUES LESS THAN (TO_DAYS('2012-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2012_01 VALUES LESS THAN (TO_DAYS('2012-02-01')),
    PARTITION p_2012_02 VALUES LESS THAN (TO_DAYS('2012-03-01')),
    PARTITION p_2012_03 VALUES LESS THAN (TO_DAYS('2012-04-01')),
    PARTITION p_2012_04 VALUES LESS THAN (TO_DAYS('2012-05-01')),
    PARTITION p_2012_05 VALUES LESS THAN (TO_DAYS('2012-06-01')),
    PARTITION p_2012_06 VALUES LESS THAN (TO_DAYS('2012-07-01')),
    PARTITION p_2012_07 VALUES LESS THAN (TO_DAYS('2012-08-01')),
    PARTITION p_2012_08 VALUES LESS THAN (TO_DAYS('2012-09-01')),
    PARTITION p_2012_09 VALUES LESS THAN (TO_DAYS('2012-10-01')),
    PARTITION p_2012_10 VALUES LESS THAN (TO_DAYS('2012-11-01')),
    PARTITION p_2012_11 VALUES LESS THAN (TO_DAYS('2012-12-01')),
    PARTITION p_2012_12 VALUES LESS THAN (TO_DAYS('2013-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2013_01 VALUES LESS THAN (TO_DAYS('2013-02-01')),
    PARTITION p_2013_02 VALUES LESS THAN (TO_DAYS('2013-03-01')),
    PARTITION p_2013_03 VALUES LESS THAN (TO_DAYS('2013-04-01')),
    PARTITION p_2013_04 VALUES LESS THAN (TO_DAYS('2013-05-01')),
    PARTITION p_2013_05 VALUES LESS THAN (TO_DAYS('2013-06-01')),
    PARTITION p_2013_06 VALUES LESS THAN (TO_DAYS('2013-07-01')),
    PARTITION p_2013_07 VALUES LESS THAN (TO_DAYS('2013-08-01')),
    PARTITION p_2013_08 VALUES LESS THAN (TO_DAYS('2013-09-01')),
    PARTITION p_2013_09 VALUES LESS THAN (TO_DAYS('2013-10-01')),
    PARTITION p_2013_10 VALUES LESS THAN (TO_DAYS('2013-11-01')),
    PARTITION p_2013_11 VALUES LESS THAN (TO_DAYS('2013-12-01')),
    PARTITION p_2013_12 VALUES LESS THAN (TO_DAYS('2014-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2014_01 VALUES LESS THAN (TO_DAYS('2014-02-01')),
    PARTITION p_2014_02 VALUES LESS THAN (TO_DAYS('2014-03-01')),
    PARTITION p_2014_03 VALUES LESS THAN (TO_DAYS('2014-04-01')),
    PARTITION p_2014_04 VALUES LESS THAN (TO_DAYS('2014-05-01')),
    PARTITION p_2014_05 VALUES LESS THAN (TO_DAYS('2014-06-01')),
    PARTITION p_2014_06 VALUES LESS THAN (TO_DAYS('2014-07-01')),
    PARTITION p_2014_07 VALUES LESS THAN (TO_DAYS('2014-08-01')),
    PARTITION p_2014_08 VALUES LESS THAN (TO_DAYS('2014-09-01')),
    PARTITION p_2014_09 VALUES LESS THAN (TO_DAYS('2014-10-01')),
    PARTITION p_2014_10 VALUES LESS THAN (TO_DAYS('2014-11-01')),
    PARTITION p_2014_11 VALUES LESS THAN (TO_DAYS('2014-12-01')),
    PARTITION p_2014_12 VALUES LESS THAN (TO_DAYS('2015-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2015_01 VALUES LESS THAN (TO_DAYS('2015-02-01')),
    PARTITION p_2015_02 VALUES LESS THAN (TO_DAYS('2015-03-01')),
    PARTITION p_2015_03 VALUES LESS THAN (TO_DAYS('2015-04-01')),
    PARTITION p_2015_04 VALUES LESS THAN (TO_DAYS('2015-05-01')),
    PARTITION p_2015_05 VALUES LESS THAN (TO_DAYS('2015-06-01')),
    PARTITION p_2015_06 VALUES LESS THAN (TO_DAYS('2015-07-01')),
    PARTITION p_2015_07 VALUES LESS THAN (TO_DAYS('2015-08-01')),
    PARTITION p_2015_08 VALUES LESS THAN (TO_DAYS('2015-09-01')),
    PARTITION p_2015_09 VALUES LESS THAN (TO_DAYS('2015-10-01')),
    PARTITION p_2015_10 VALUES LESS THAN (TO_DAYS('2015-11-01')),
    PARTITION p_2015_11 VALUES LESS THAN (TO_DAYS('2015-12-01')),
    PARTITION p_2015_12 VALUES LESS THAN (TO_DAYS('2016-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2016_01 VALUES LESS THAN (TO_DAYS('2016-02-01')),
    PARTITION p_2016_02 VALUES LESS THAN (TO_DAYS('2016-03-01')),
    PARTITION p_2016_03 VALUES LESS THAN (TO_DAYS('2016-04-01')),
    PARTITION p_2016_04 VALUES LESS THAN (TO_DAYS('2016-05-01')),
    PARTITION p_2016_05 VALUES LESS THAN (TO_DAYS('2016-06-01')),
    PARTITION p_2016_06 VALUES LESS THAN (TO_DAYS('2016-07-01')),
    PARTITION p_2016_07 VALUES LESS THAN (TO_DAYS('2016-08-01')),
    PARTITION p_2016_08 VALUES LESS THAN (TO_DAYS('2016-09-01')),
    PARTITION p_2016_09 VALUES LESS THAN (TO_DAYS('2016-10-01')),
    PARTITION p_2016_10 VALUES LESS THAN (TO_DAYS('2016-11-01')),
    PARTITION p_2016_11 VALUES LESS THAN (TO_DAYS('2016-12-01')),
    PARTITION p_2016_12 VALUES LESS THAN (TO_DAYS('2017-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2017_01 VALUES LESS THAN maxvalue
    );
## -------------------------------------------------------------------------- ##   


################################################################################～～～～～～～～～～～～～
## FromDC.minute                                                   ## FromDC.minute
################################################################################～～～～～～～～～～～～～

CREATE TABLE  FromDC.minute(
    TradingDay       DATE           NOT NULL,              ## 交易日期
    Minute           TIME           NOT NULL,              ## 分钟，格式为==> "HH:MM:SS"", 与 Wind 数据库类似
    NumericExchTime  DECIMAL(15,5)  NOT NULL,              ## 分钟的数值格式，以 18:00::00 为正负界限，
    #                                                      ## 注意：取的是有 tick 的第一个，不一定是这个分钟开始的值
    #                                                      ## 为了方便 order：
    #                                                      ## 1. 负值表示夜盘的分钟
    #                                                      ## 2. 正值表示日盘的分钟
    InstrumentID     CHAR(30)   NOT NULL,                  ## 合约名称
    #------------------------------------------------------
    OpenPrice        DECIMAL(15,5)          NULL,          ## 开盘价
    HighPrice        DECIMAL(15,5)          NULL,          ## 最高价
    LowPrice         DECIMAL(15,5)          NULL,          ## 最低价
    ClosePrice       DECIMAL(15,5)          NULL,          ## 收盘价
    #-----------------------------------------------------
    Volume           INT UNSIGNED           NULL,          ## 成交量
    Turnover         DECIMAL(30,3)          NULL,          ## 成交额
    #-----------------------------------------------------
    OpenOpenInterest  INT UNSIGNED          NULL,          ## 分钟的开仓的开盘量
    HighOpenInterest  INT UNSIGNED          NULL,          ## 分钟的开仓的最高量
    LowOpenInterest   INT UNSIGNED          NULL,          ## 分钟的开仓的最低量
    CloseOpenInterest INT UNSIGNED          NULL,          ## 分钟的开仓的收盘量，即 position
    #-----------------------------------------------------
    UpperLimitPrice  DECIMAL(15,5)          NULL,          ## 当日的有效最高报价
    LowerLimitPrice  DECIMAL(15,5)          NULL,          ## 当日的有效最低报价
    SettlementPrice  DECIMAL(15,5)          NULL,          ## 当日交易所公布的结算价
    #-----------------------------------------------------
    PRIMARY KEY (TradingDay, Minute, InstrumentID)         ## 主键唯一，重复不可输入
    );

##----------- INDEX --------------------------------------------------------- ##
CREATE INDEX index_minute
ON FromDC.minute
(TradingDay, Minute, InstrumentID);  
## -------------------------------------------------------------------------- ## 

##----------- PARTITIONS ---------------------------------------------------- ##
ALTER TABLE FromDC.minute
    PARTITION BY RANGE( TO_DAYS(TradingDay) )(
    #---------------------------------------------------------------------------
    PARTITION p_2010_01 VALUES LESS THAN (TO_DAYS('2010-02-01')),
    PARTITION p_2010_02 VALUES LESS THAN (TO_DAYS('2010-03-01')),
    PARTITION p_2010_03 VALUES LESS THAN (TO_DAYS('2010-04-01')),
    PARTITION p_2010_04 VALUES LESS THAN (TO_DAYS('2010-05-01')),
    PARTITION p_2010_05 VALUES LESS THAN (TO_DAYS('2010-06-01')),
    PARTITION p_2010_06 VALUES LESS THAN (TO_DAYS('2010-07-01')),
    PARTITION p_2010_07 VALUES LESS THAN (TO_DAYS('2010-08-01')),
    PARTITION p_2010_08 VALUES LESS THAN (TO_DAYS('2010-09-01')),
    PARTITION p_2010_09 VALUES LESS THAN (TO_DAYS('2010-10-01')),
    PARTITION p_2010_10 VALUES LESS THAN (TO_DAYS('2010-11-01')),
    PARTITION p_2010_11 VALUES LESS THAN (TO_DAYS('2010-12-01')),
    PARTITION p_2010_12 VALUES LESS THAN (TO_DAYS('2011-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2011_01 VALUES LESS THAN (TO_DAYS('2011-02-01')),
    PARTITION p_2011_02 VALUES LESS THAN (TO_DAYS('2011-03-01')),
    PARTITION p_2011_03 VALUES LESS THAN (TO_DAYS('2011-04-01')),
    PARTITION p_2011_04 VALUES LESS THAN (TO_DAYS('2011-05-01')),
    PARTITION p_2011_05 VALUES LESS THAN (TO_DAYS('2011-06-01')),
    PARTITION p_2011_06 VALUES LESS THAN (TO_DAYS('2011-07-01')),
    PARTITION p_2011_07 VALUES LESS THAN (TO_DAYS('2011-08-01')),
    PARTITION p_2011_08 VALUES LESS THAN (TO_DAYS('2011-09-01')),
    PARTITION p_2011_09 VALUES LESS THAN (TO_DAYS('2011-10-01')),
    PARTITION p_2011_10 VALUES LESS THAN (TO_DAYS('2011-11-01')),
    PARTITION p_2011_11 VALUES LESS THAN (TO_DAYS('2011-12-01')),
    PARTITION p_2011_12 VALUES LESS THAN (TO_DAYS('2012-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2012_01 VALUES LESS THAN (TO_DAYS('2012-02-01')),
    PARTITION p_2012_02 VALUES LESS THAN (TO_DAYS('2012-03-01')),
    PARTITION p_2012_03 VALUES LESS THAN (TO_DAYS('2012-04-01')),
    PARTITION p_2012_04 VALUES LESS THAN (TO_DAYS('2012-05-01')),
    PARTITION p_2012_05 VALUES LESS THAN (TO_DAYS('2012-06-01')),
    PARTITION p_2012_06 VALUES LESS THAN (TO_DAYS('2012-07-01')),
    PARTITION p_2012_07 VALUES LESS THAN (TO_DAYS('2012-08-01')),
    PARTITION p_2012_08 VALUES LESS THAN (TO_DAYS('2012-09-01')),
    PARTITION p_2012_09 VALUES LESS THAN (TO_DAYS('2012-10-01')),
    PARTITION p_2012_10 VALUES LESS THAN (TO_DAYS('2012-11-01')),
    PARTITION p_2012_11 VALUES LESS THAN (TO_DAYS('2012-12-01')),
    PARTITION p_2012_12 VALUES LESS THAN (TO_DAYS('2013-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2013_01 VALUES LESS THAN (TO_DAYS('2013-02-01')),
    PARTITION p_2013_02 VALUES LESS THAN (TO_DAYS('2013-03-01')),
    PARTITION p_2013_03 VALUES LESS THAN (TO_DAYS('2013-04-01')),
    PARTITION p_2013_04 VALUES LESS THAN (TO_DAYS('2013-05-01')),
    PARTITION p_2013_05 VALUES LESS THAN (TO_DAYS('2013-06-01')),
    PARTITION p_2013_06 VALUES LESS THAN (TO_DAYS('2013-07-01')),
    PARTITION p_2013_07 VALUES LESS THAN (TO_DAYS('2013-08-01')),
    PARTITION p_2013_08 VALUES LESS THAN (TO_DAYS('2013-09-01')),
    PARTITION p_2013_09 VALUES LESS THAN (TO_DAYS('2013-10-01')),
    PARTITION p_2013_10 VALUES LESS THAN (TO_DAYS('2013-11-01')),
    PARTITION p_2013_11 VALUES LESS THAN (TO_DAYS('2013-12-01')),
    PARTITION p_2013_12 VALUES LESS THAN (TO_DAYS('2014-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2014_01 VALUES LESS THAN (TO_DAYS('2014-02-01')),
    PARTITION p_2014_02 VALUES LESS THAN (TO_DAYS('2014-03-01')),
    PARTITION p_2014_03 VALUES LESS THAN (TO_DAYS('2014-04-01')),
    PARTITION p_2014_04 VALUES LESS THAN (TO_DAYS('2014-05-01')),
    PARTITION p_2014_05 VALUES LESS THAN (TO_DAYS('2014-06-01')),
    PARTITION p_2014_06 VALUES LESS THAN (TO_DAYS('2014-07-01')),
    PARTITION p_2014_07 VALUES LESS THAN (TO_DAYS('2014-08-01')),
    PARTITION p_2014_08 VALUES LESS THAN (TO_DAYS('2014-09-01')),
    PARTITION p_2014_09 VALUES LESS THAN (TO_DAYS('2014-10-01')),
    PARTITION p_2014_10 VALUES LESS THAN (TO_DAYS('2014-11-01')),
    PARTITION p_2014_11 VALUES LESS THAN (TO_DAYS('2014-12-01')),
    PARTITION p_2014_12 VALUES LESS THAN (TO_DAYS('2015-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2015_01 VALUES LESS THAN (TO_DAYS('2015-02-01')),
    PARTITION p_2015_02 VALUES LESS THAN (TO_DAYS('2015-03-01')),
    PARTITION p_2015_03 VALUES LESS THAN (TO_DAYS('2015-04-01')),
    PARTITION p_2015_04 VALUES LESS THAN (TO_DAYS('2015-05-01')),
    PARTITION p_2015_05 VALUES LESS THAN (TO_DAYS('2015-06-01')),
    PARTITION p_2015_06 VALUES LESS THAN (TO_DAYS('2015-07-01')),
    PARTITION p_2015_07 VALUES LESS THAN (TO_DAYS('2015-08-01')),
    PARTITION p_2015_08 VALUES LESS THAN (TO_DAYS('2015-09-01')),
    PARTITION p_2015_09 VALUES LESS THAN (TO_DAYS('2015-10-01')),
    PARTITION p_2015_10 VALUES LESS THAN (TO_DAYS('2015-11-01')),
    PARTITION p_2015_11 VALUES LESS THAN (TO_DAYS('2015-12-01')),
    PARTITION p_2015_12 VALUES LESS THAN (TO_DAYS('2016-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2016_01 VALUES LESS THAN (TO_DAYS('2016-02-01')),
    PARTITION p_2016_02 VALUES LESS THAN (TO_DAYS('2016-03-01')),
    PARTITION p_2016_03 VALUES LESS THAN (TO_DAYS('2016-04-01')),
    PARTITION p_2016_04 VALUES LESS THAN (TO_DAYS('2016-05-01')),
    PARTITION p_2016_05 VALUES LESS THAN (TO_DAYS('2016-06-01')),
    PARTITION p_2016_06 VALUES LESS THAN (TO_DAYS('2016-07-01')),
    PARTITION p_2016_07 VALUES LESS THAN (TO_DAYS('2016-08-01')),
    PARTITION p_2016_08 VALUES LESS THAN (TO_DAYS('2016-09-01')),
    PARTITION p_2016_09 VALUES LESS THAN (TO_DAYS('2016-10-01')),
    PARTITION p_2016_10 VALUES LESS THAN (TO_DAYS('2016-11-01')),
    PARTITION p_2016_11 VALUES LESS THAN (TO_DAYS('2016-12-01')),
    PARTITION p_2016_12 VALUES LESS THAN (TO_DAYS('2017-01-01')),
    #---------------------------------------------------------------------------
    #---------------------------------------------------------------------------
    PARTITION p_2017_01 VALUES LESS THAN maxvalue
    );
## -------------------------------------------------------------------------- ##   


################################################################################～～～～～～～～～～～～
## dev.FromDC_breakTime                                                      ## dev.FromDC_breakTime
################################################################################～～～～～～～～～～～～

CREATE TABLE FromDC.breakTime(
    TradingDay   DATE      NOT      NULL,                  ## 交易日期
    beginTime    TIME      NOT     NULL,                  ## 数据中断开始的时间
    endTime      TIME      NOT     NULL,                  ## 数据中断结束的时间
    #-----------------------------------------------------
    DataSource   CHAR(20)  NOT      NULL,                  ## 原始数据文件的来源，为主要目录
    DataFile     CHAR(20)  NOT      NULL,                  ## 原始数据的文件，为 csv 文件/路径
    #-----------------------------------------------------
    PRIMARY KEY (TradingDay, beginTime, endTime,
                 DataSource, DataFile)                     ## 主键唯一，重复不可输入
    );

################################################################################～～～～～～～～～～～～
## dev.FromDC_log                                                             ## dev.FromDC_log
################################################################################～～～～～～～～～～～～

CREATE TABLE FromDC.log(
    TradingDay   DATE           NOT      NULL,             ## 交易日期
    Sector       CHAR(20)       NOT      NULL,             ## 输入的数据类型：
    #                                                      ## 1. 'daily':主要处理日数据
    #                                                      ## 2. 'minute':分钟级别的数据
    #-----------------------------------------------------
    User         TINYTEXT           NULL,                  ## 哪个账户在录入数据
    MysqlDB      TINYTEXT           NULL,                  ## 数据输入到哪个数据库
    DataSource   TINYTEXT  NOT      NULL,                  ## 原始数据文件的来源，为主要目录
    DataFile     TEXT      NOT      NULL,                  ## 原始数据的文件，为 csv 文件/路径
    #-----------------------------------------------------
    RscriptMain  TEXT      NOT      NULL,                  ## 使用的主要 R 脚本文件，为最上层的文件，包括需要的包、相应的配置
    RscriptSub   TEXT      NOT      NULL,                  ## 使用的次一级 R 脚本，主要包括编写的函数即各种算法
    ProgBeginTime    DATETIME  NOT      NULL,              ## 程序开始运行的时间
    ProgEndTime      DATETIME  NOT      NULL,              ## 程序结束运行的时间
    Results      TEXT               NULL,                  ## 对数据哭修改的内容记录
    Remarks      TEXT               NULL,                  ## 备注，方便日后添加说明
    #-----------------------------------------------------
    PRIMARY KEY (TradingDay, Sector)                       ## 主键唯一，重复不可输入
    );

################################################################################～～～～～～～～～～～～
## dev.FromDC_log                                                             ## dev.FromDC_log
################################################################################～～～～～～～～～～～～

CREATE TABLE FromDC.info(
    TradingDay       DATE           NOT NULL,              ## 交易日期
    InstrumentID     CHAR(30)       NOT NULL,              ## 合约名称
    PriceTick        DECIMAL(10,5)  ,           
    VolumeMultiple   mediumint      ,     
    #-----------------------------------------------------
    PRIMARY KEY (TradingDay, InstrumentID)                 ## 主键唯一，重复不可输入
    );


################################################################################
truncate table daily;
truncate table minute;
truncate table breakTime;
truncate table info;
truncate table log;
