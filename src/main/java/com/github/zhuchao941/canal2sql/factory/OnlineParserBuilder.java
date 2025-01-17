package com.github.zhuchao941.canal2sql.factory;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.github.zhuchao941.canal2sql.filter.LogEventFilter;
import com.github.zhuchao941.canal2sql.parser.MysqlOnlineEventParser;
import com.github.zhuchao941.canal2sql.starter.Configuration;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;

public class OnlineParserBuilder {

    private Configuration configuration;

    public OnlineParserBuilder(Configuration configuration) {
        this.configuration = configuration;
    }

    public MysqlOnlineEventParser build() {
        String startPositionStr = configuration.getStartPosition();
        String endPositionStr = configuration.getEndPosition();

        FileWithPosition startFileWithPosition = StringUtils.isNotBlank(startPositionStr) ? extract(startPositionStr) : new FileWithPosition();
        FileWithPosition endFileWithPosition = StringUtils.isNotBlank(endPositionStr) ? extract(endPositionStr) : new FileWithPosition();

        MysqlOnlineEventParser parser = new MysqlOnlineEventParser();
        parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configuration.getHost(), configuration.getPort()), configuration.getUsername(), configuration.getPassword()));
        // 这里直接指定startPosition性能更好
        if (startFileWithPosition != null) {
            parser.setMasterPosition(new EntryPosition(startFileWithPosition.getFileName(), startFileWithPosition.getPosition()));
        }
        parser.setLogEventFilter(new LogEventFilter(configuration.getStartDatetime(), configuration.getEndDatetime(), startFileWithPosition.getPosition(), endFileWithPosition.getPosition(), startFileWithPosition.getFileName(), endFileWithPosition.getFileName(), configuration.getDataFilter()));
        return parser;
    }

    class FileWithPosition {
        private String fileName;
        private Long position;

        public FileWithPosition() {
        }

        public FileWithPosition(String fileName, Long position) {
            this.fileName = fileName;
            this.position = position;
        }

        public String getFileName() {
            return fileName;
        }

        public Long getPosition() {
            return position;
        }
    }

    private FileWithPosition extract(String fileWithPositionStr) {
        String[] split = fileWithPositionStr.split("\\|");
        if (split.length != 2) {
            throw new IllegalArgumentException("must fileName|position");
        } else {
            return new FileWithPosition(split[0], Long.parseLong(split[1]));
        }
    }
}
