import lombok.Getter;
import lombok.Setter;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

import java.util.List;

/**
 * @ProjectName: jd_shop_parent
 * @program: PACKAGE_NAME
 * @FileName: Logparsing_test
 * @description: Logparsing日志解析器示例demo
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-29 14:21
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class Logparsing_test {

    public static void main(String[] args) throws Exception {
        //定义需要解析的数据样本
        String logData = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - " +
                "[05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 " +
                "\"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) " +
                "AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; " +
                "BuI=SomeThing; Apache=127.0.0.1.1351111543699529\" \"广州\"";
        //定义数据的解析规则
        String logFormat = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\" \"%{Addr}i\"";

        //创建日志解析器
        Parser<HttpdLogRecord> parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat);

        //打印出能解析出来的所有参数信息
        //printAllPossibles(logFormat);

        //将JavaBean的变量和解析出的字段进行映射关联
        parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user");
        parser.addParseTarget("setConnectionClientHost", "IP:connection.client.host");
        parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.original.method");
        parser.addParseTarget("setStatus", "STRING:request.status.last");
        parser.addParseTarget("setAddr", "HTTP.HEADER:request.header.addr");

       HttpdLogRecord record = new HttpdLogRecord();

        //将解析的数据封装到JavaBean中
        parser.parse(record, logData);

        System.out.println(record.toString());


        System.out.println(record.getConnectionClientUser());
        System.out.println(record.getConnectionClientHost());
        System.out.println(record.getMethod());
        System.out.println(record.getStatus());
        System.out.println(record.getAddr());

    }

    /**
     * 打印出能解析出来的所有参数信息
     *
     * @param logformat
     * @throws NoSuchMethodException
     * @throws MissingDissectorsException
     * @throws InvalidDissectorException
     */
    public static void printAllPossibles(String logformat) throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {
        // 为了弄清楚我们可以从这一行得到什么值，我们使用一个没有ANY @Field注释的伪类实例化解析器。
        Parser<Object> dummyParser = new HttpdLoglineParser<>(Object.class, logformat);

        //用于查询解析器中可提取的所有参数的信息
        List<String> possiblePaths;
        possiblePaths = dummyParser.getPossiblePaths();

        dummyParser.addParseTarget(String.class.getMethod("indexOf", String.class), possiblePaths);

        System.out.println("==================================");
        System.out.println("Possible output:");
        for (String path : possiblePaths) {
            System.out.println(path + "      " + dummyParser.getCasts(path));
        }
        System.out.println("==================================");
    }

    /**
     * JavaBean对象
     */
    public static class HttpdLogRecord {
        @Setter
        @Getter
        private String connectionClientUser = null;
        @Getter
        @Setter
        private String connectionClientHost = null;
        @Getter
        @Setter
        private String method = null;
        @Getter
        @Setter
        private String status = null;
        @Getter
        @Setter
        private String addr = null;

        @Override
        public String toString() {
            return "HttpdLogRecord{" +
                    "connectionClientUser='" + connectionClientUser + '\'' +
                    ", connectionClientHost='" + connectionClientHost + '\'' +
                    ", method='" + method + '\'' +
                    ", status='" + status + '\'' +
                    ", addr='" + addr + '\'' +
                    '}';
        }
    }

}

