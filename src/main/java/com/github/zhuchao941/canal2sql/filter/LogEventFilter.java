package com.github.zhuchao941.canal2sql.filter;

import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.List;

public class LogEventFilter {

    private long serverId;
    private Date startTime;
    private Date endTime;
    private Long startPosition;
    private Long endPosition;

    private String startFile;
    private String endFile;

    private String column;
    private String columnValue;

    public LogEventFilter(Date startTime, Date endTime, Long startPosition, Long endPosition, String dataFilter) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        parseDataFilter(dataFilter);
    }

    public LogEventFilter(Date startTime, Date endTime, Long startPosition, Long endPosition, String startFile, String endFile, String dataFilter) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.startFile = startFile;
        this.endFile = endFile;
        parseDataFilter(dataFilter);
    }

    private void parseDataFilter(String dataFilter) {
        if (StringUtils.isEmpty(dataFilter)) {
            return;
        }
        if (!dataFilter.contains(":")) {
            this.column = "id";
            this.columnValue = dataFilter;
        } else {
            String[] strs = dataFilter.split(":");
            this.column = strs[0];
            this.columnValue = strs[1];
        }
    }

    public LogEvent filter(LogEvent event, CanalEntry.Entry entry) {
        if (event == null) {
            return null;
        }
        String logFileName = event.getHeader().getLogFileName();
        if (startTime != null && event.getWhen() < startTime.getTime() / 1000) {
            return null;
        }
        if (endTime != null && event.getWhen() > endTime.getTime() / 1000) {
            shutdownNow();
            return null;
        }
        // binlog 文件需要从头遍历，而online模式可以直接从指定位置读
        if (startFile == null) {
            if (startPosition != null && event.getLogPos() < startPosition) {
                return null;
            }
        } else {
            if (startFile.equals(logFileName) && startPosition != null && event.getLogPos() < startPosition) {
                return null;
            }
        }

        if (endFile == null) {
            if (endPosition != null && event.getLogPos() > endPosition) {
                shutdownNow();
                return null;
            }
        } else {
            if (endFile.equals(logFileName) && endPosition != null && event.getLogPos() > endPosition) {
                shutdownNow();
                return null;
            }
        }

        if (serverId != 0 && event.getServerId() != serverId) {
            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
        }
        boolean dataFilter = filterData(entry);
        return dataFilter ? event : null;
    }

    private boolean filterData(CanalEntry.Entry entry) {
        if (StringUtils.isEmpty(this.column) || StringUtils.isEmpty(this.columnValue)) {
            return true;
        }
        if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
            return true;
        }
        CanalEntry.RowChange rowChage = null;
        try {
            rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
        }
        if (rowChage.getIsDdl()) {
            return false;
        }
        List<CanalEntry.RowData> rowDatasList = rowChage.getRowDatasList();
        for (CanalEntry.RowData data : rowDatasList) {
            CanalEntry.Column column = data.getAfterColumnsList().stream().filter(e -> e.getName().equalsIgnoreCase(this.column)).findAny().orElse(null);
            if (column == null) {
                break;
            }
            if (column.getValue().equals(this.columnValue)) {
                return true;
            }
        }
        return false;
    }

    private void shutdownNow() {
        System.exit(1);
    }
}
