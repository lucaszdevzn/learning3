################################################################################
## FundReporting.R
##
## 用于 基金交易 汇报
##
##
## 注意:
##
## Author: fl@hicloud-investment.com
## CreateDate: 2017-11-14
################################################################################

################################################################################
## STEP 0: 初始化，载入包，设定初始条件
################################################################################
rm(list = ls())

setwd('/home/fl/myData/')
suppressMessages({
  source('./R/Rconfig/myInit.R')
})

## =============================================================================
ChinaFuturesCalendar <- fread("./data/ChinaFuturesCalendar/ChinaFuturesCalendar.csv",
                              colClasses = list(character = c("nights","days")))
currTradingDay <- ChinaFuturesCalendar[days <= format(Sys.Date(), '%Y%m%d')][nights < format(Sys.Date(), '%Y%m%d')][.N]
## =============================================================================

accountAll <- data.table(
    accountID   = c('TianMi2','TianMi3','YunYang1','YongAnLYB'),
    accountName = c('甜蜜2号','甜蜜3号','云扬1号','永安林艳彬')
    )

logPath <- "/home/fl/myData/log/FundReporting"

tempFile <- paste0(logPath,'/',currTradingDay[1, days],'_fund.txt')
if (file.exists(tempFile)) file.remove(tempFile)
  tempFile <- paste0(logPath,'/',currTradingDay[1, days],'.txt')
if (file.exists(tempFile)) file.remove(tempFile)



navCalculating <- function(fundID) {
    mysql <- mysqlFetch(fundID, host = '192.168.1.135')

    ## 如果右 nav 这个数据表
    ## 说明有对理财数据进行爬虫处理，
    ## 如　YunYang1 基金
    dtNav <- dbGetQuery(mysql, 
        "select * from nav order by TradingDay") %>% 
        as.data.table()
    ## 分开讨论两种情况
    
    ## 由交易系统保存的信息
    dtAccount <- dbGetQuery(mysql, 
        "select * from accountInfo order by TradingDay") %>% 
        as.data.table()

    if (nrow(dtNav) == 0) {
        ## 基金手续费
        fee <- dbGetQuery(mysql, "select * from fee order by TradingDay") %>% 
            as.data.table()
        dtAccount <- merge(dtAccount, fee, by = 'TradingDay', all.x = TRUE)

        ## 基金申购与赎回
        funding <- dbGetQuery(mysql, "select * from funding order by TradingDay") %>% 
            as.data.table()
        dtAccount <- merge(dtAccount, funding, by = 'TradingDay', all.x = TRUE)

        ## --------------------
        ## 开始处理 nav 净值计算
        ## --------------------
        dtNav <- dtAccount[, .(
            TradingDay, accountID, updateTime, 
            balance, deposit, withdraw,
            flowCapital, banking, fee = ifelse(is.na(Amount),0,as.numeric(Amount)),
            shares = ifelse(is.na(shares.y), 0, shares.y)
            )]
        dtNav[, ":="(
            assets = balance + flowCapital + banking + cumsum(fee),
            shares = cumsum(shares)
            )]
        dtNav[, nav := assets / shares]

    } else {
        dtNav <- merge(dtNav, 
                       dtAccount[, .(TradingDay, accountID, updateTime, 
                                     balance, deposit, withdraw, fee)],
                       by = 'TradingDay', all.x = TRUE)
        dtNav <- dtNav[, .(TradingDay, accountID, updateTime,
                           balance, deposit, withdraw,
                           flowCapital = Currency,
                           banking = Bank,
                           fee,
                           assets = Futures + Currency + Bank,
                           shares = Shares)]
        dtNav[, nav := assets / shares]
    }

    ## =========================================================================
    ## 是否是周数据
    if (F) {
        dtNav[, ":="(
            TradingYear = substr(TradingDay, 1, 4),
            TradingWeek = lubridate::week(TradingDay)
          )]
        nanhua[, ":="(
            TradingYear = substr(TradingDay, 1, 4),
            TradingWeek = lubridate::week(TradingDay)
          )]  

        currWeek <- lubridate::week(Sys.Date())
        preWeek <- currWeek - 1
        nav_currWeek <- dtNav[TradingYear == format(Sys.Date(), "%Y")][TradingWeek == currWeek][.N]
        nav_preWeek <- dtNav[TradingYear == format(Sys.Date(), "%Y")][TradingWeek == preWeek][.N]
        deltaNav <- nav_currWeek$nav - nav_preWeek$nav   

        nanhua_currWeek <- nanhua[TradingYear == format(Sys.Date(), "%Y")][TradingWeek == currWeek][.N]
        nanhua_preWeek <- nanhua[TradingYear == format(Sys.Date(), "%Y")][TradingWeek == preWeek][.N]
        deltaNanhua <- nanhua_currWeek$close / nanhua_preWeek$close - 1
    }
    ## =========================================================================

    dtNav[, TradingDay := as.Date(TradingDay)] %>% 
        .[, rtn := c(0, diff(nav)/.SD[1:(.N-1), nav])] %>%
        .[, ":="(
            maxNav = cummax(nav),
            DD  = nav / cummax(nav) - 1,
            maxDD = cummin((nav - cummax(nav)) / cummax(nav))
        )]

    ## ----------------------------------------
    for ( conns in dbListConnections(MySQL()) ) dbDisconnect(conns)
    ## ----------------------------------------

    ## -----------
    return(dtNav)
    ## -----------
}
## =============================================================================
## i = 1
fetchFund <- function(fundID, author = FALSE) {
    ## ----------------------------
    dtNav <- navCalculating(fundID)
    ## ----------------------------

    ## -------------------------------------------------------------------------
    fundPerformance <- data.table(
        基金名称 = accountAll[accountID == fundID, accountName],
        期货账户 = prettyNum(dtNav[.N, balance + sum(fee)], big.mark = ','),
        理财账户 = prettyNum(dtNav[.N, flowCapital], big.mark = ','),
        银行存款 = prettyNum(dtNav[.N, banking], big.mark = ','),
        账户总额 = prettyNum(dtNav[.N, assets], big.mark = ','),
        今日盈亏 = ifelse(nrow(dtNav) > 1,
                      prettyNum(
                        dtNav[.N, assets - (deposit - withdraw)] -
                        dtNav[.N-1, assets - (deposit - withdraw)], big.mark = ','),
                          0),
        收益波动 = paste0(as.character(dtNav[.N, round(rtn * 100,2)]),'%'),
        基金净值 = dtNav[.N, round(nav,4)]
        )
    ## -------------------------------------------------------------------------

    mysql <- mysqlFetch(fundID, host = '192.168.1.135')

    ## -------------------------------------------------------------------------
    positionInfo <- dbGetQuery(mysql,
        "select * from positionInfo") %>% as.data.table() %>%
        .[, .(volume = .SD[, sum(volume)])
          , by = c('strategyID','InstrumentID', 'direction')] %>%
        .[order(InstrumentID)]
    # print(positionInfo)
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    tradingInfo <- dbGetQuery(mysql, paste(
        "select * from tradingInfo
        where TradingDay = ", currTradingDay[1, days])) %>%
        as.data.table() %>%
        .[, TradingDay := NULL]
    # print(tradingInfo)
    ## -------------------------------------------------------------------------


    ## -------------------------------------------------------------------------
    failedInfo <- dbGetQuery(mysql,
        "select * from failedInfo") %>% as.data.table()
    failedInfo[offset == '平仓', direction := ifelse(direction == 'long','short','short')]
    failedInfo[, offset := NULL]
    setcolorder(failedInfo, c('TradingDay', 'strategyID', 'InstrumentID', 'direction','volume'))
    # print(positionInfo)
    ## -------------------------------------------------------------------------

    ## ---------------------------------------------------------------------------
    ## 写入 log
    tempFile <- paste0(logPath,'/',currTradingDay[1, days],'_fund.txt')
    if (!grepl('SimNow', accountAll[i,accountID])) {
        sink(tempFile, append = TRUE)
        cat("## ----------------------------------- ##")
        cat('\n')
        write.table(as.data.frame(t(fundPerformance)), tempFile
                    , append = TRUE, col.names = FALSE
                    , sep = ' :==> ')
        if (!is.na(grep('SimNow', accountAll$accountID)[1])) {
            if (i == grep('SimNow', accountAll$accountID)[1]-1) {
                cat("## ----------------------------------- ##\n")
                cat("\n## ----------------------------------- ##\n")
                cat("## @william")
            }
        }
    }

    ## ===========================================================================
    tempFile <- paste0(logPath,'/',currTradingDay[1, days],'.txt')
    sink(tempFile, append = TRUE)
    cat("## ----------------------------------- ##")
    cat('\n')
    cat(paste0('## >>>>>>>>> ',fundID))
    cat('\n')
    cat("## ----------------------------------- ##")
    cat('\n')
    write.table(as.data.frame(t(fundPerformance)), tempFile
                , append = TRUE, col.names = FALSE
                , sep = ' :==> ')
    cat('\n')
    if (nrow(positionInfo) != 0) {
        cat("## ----------------------------------- ##")
        cat("\n## 今日持仓信息 ##")
        cat('\n')
        print(positionInfo)
        cat('\n')
    }  

    if (nrow(failedInfo) != 0) {
        cat("## ----------------------------------- ##")
        cat("\n## 未平仓信息 ##")
        cat('\n')
        print(failedInfo)
        cat('\n')
    }

    ## ===========================================================================
    ## 不看交易记录
    # if (nrow(tradingInfo) != 0) {
    #   write.table("## -------------------------------------- ##", tempFile
    #               , append = TRUE, col.names = FALSE, row.names = FALSE)
    #   cat("## 今日交易记录 ##")
    #   cat('\n')
    #   print(tradingInfo)
    #   cat('\n')
    # }
    cat("## ----------------------------------- ##\n")
    ## ===========================================================================
    if (author) {
        cat("\n## ----------------------------------- ##\n")
        cat("## @william")
    }
}
## =============================================================================

for (i in 1:nrow(accountAll)) {
    # print(i)
    if (i < nrow(accountAll)) {
        fetchFund(accountAll[i, accountID])
    } else {
        fetchFund(accountAll[i, accountID], author = TRUE)
    }
}
## ----------------------------------------
for ( conns in dbListConnections(MySQL()) ) dbDisconnect(conns)
## ----------------------------------------
