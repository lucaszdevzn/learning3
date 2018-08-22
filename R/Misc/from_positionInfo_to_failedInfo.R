rm(list = ls())

suppressMessages({
  source('/home/fl/myData/R/Rconfig/myInit.R')
})

print(currTradingDay)
print(lastTradingDay)

allAccount <- c('TianMi2','TianMi3','YunYang1','RuiLong1')

for (x in allAccount) {
    ## ----------------------------------------------
    mysql <- mysqlFetch(db = x, host = '192.168.1.188')
    dbSendQuery(mysql, "truncate table positionInfo")
    dbSendQuery(mysql, "truncate table failedInfo")
    ## ----------------------------------------------
    # processFail(x)
}
## Run


processFail <- function(fundID, fundHost = '192.168.1.188') {
    # fundID <- 'TianMi3'
    # fundHost <- '192.168.1.188'
    ## -------------------------------------------------------------------------
    dtSignal <- mysqlQuery(db = fundID,
                           query = "select * from tradingSignal",
                           host = fundHost)
    print(dtSignal)
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    dtPositionInfo <- mysqlQuery(db = fundID,
                                 query = "select * from positionInfo",
                                 host = fundHost)
    print(dtPositionInfo)
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    dtFailedInfo <- data.table(strategyID = '',
                               InstrumentID = '',
                               TradingDay = '',
                               direction = '',
                               offset = '',
                               volume = 0)
    print(dtFailedInfo)
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    for (i in 1:nrow(dtPositionInfo)) {
      tempInstrumentID <- dtPositionInfo[i, InstrumentID]
      tempDirection <- ifelse(dtPositionInfo[i, direction] == 'long', 1, -1)

      if (tempInstrumentID %in% dtSignal$InstrumentID) {
        tempSignal <- dtSignal[InstrumentID == tempInstrumentID]

        if (tempDirection == tempSignal[1, direction]) {
          diffVolume <- dtPositionInfo[i, volume] - tempSignal[1, volume]
          if (diffVolume >= 0) {
            res <- dtPositionInfo[i, .(strategyID, InstrumentID = tempInstrumentID,
                                       TradingDay, direction = ifelse(tempDirection == 'long', 'short', 'long'),
                                       offset = '平仓',
                                       volume = abs(diffVolume))]
            dtFailedInfo <- rbind(dtFailedInfo, res)
            dtPositionInfo[i, ":="(
              volume = tempSignal[1,volume],
              TradingDay = currTradingDay[1,days]
            )]
          } else {
            dtPositionInfo[i, ":="(
              TradingDay = currTradingDay[1,days]
            )]
          }
        } else {
          res <- dtPositionInfo[i, .(strategyID, InstrumentID,
                                     TradingDay, 
                                     direction = ifelse(tempDirection == 1, 'short', 'long'),
                                     offset = 'close',
                                     volume)]
          dtFailedInfo <- rbind(dtFailedInfo, res)
        }
      } else {
        res <- dtPositionInfo[i, .(strategyID, InstrumentID,
                                   TradingDay, direction = ifelse(tempDirection == 1, 'short', 'long'),
                                   offset = 'close',
                                   volume)]
        dtFailedInfo <- rbind(dtFailedInfo, res)
      }

    }
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    dtFailedInfo <- dtFailedInfo[volume != 0]
    dtPositionInfo <- dtPositionInfo[TradingDay == currTradingDay[1,days]]
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    print(dtSignal)
    print(dtFailedInfo)
    print(dtPositionInfo)
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    mysql <- mysqlFetch(db = fundID, host = fundHost)

    dbSendQuery(mysql, "truncate table positionInfo")
    dbWriteTable(mysql, 'positionInfo', dtPositionInfo, row.name = F, append = T)

    if (nrow(dtFailedInfo) != 0) {
        dbSendQuery(mysql, "truncate table failedInfo")
        dbWriteTable(mysql, 'failedInfo', dtFailedInfo, row.name = F, append = T)
    }
    # ## -------------------------------------------------------------------------
}


allAccount <- c('TianMi2','TianMi3','YunYang1','RuiLong1')
for (x in allAccount) {
    processFail(x)
}
