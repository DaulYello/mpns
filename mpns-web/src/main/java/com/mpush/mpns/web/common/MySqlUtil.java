package com.mpush.mpns.web.common;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.List;


/**
 * Created by zzl on 2017/11/20.
 */
public class MySqlUtil {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected SQLClient client;

    @Resource
    protected Vertx vertx;

    // 数据库...
    public void init() {
        JsonObject object = new JsonObject()
                .put("host",host)
                .put("port",port)
                .put("username",username)
                .put("password",password)
                .put("maxPollSize",maxPollSize)
                .put("database",database);
        client = MySQLClient.createShared(vertx,object);
    }


    public Future<SQLConnection> getConnection() {
        Future<SQLConnection> future = Future.future();
        client.getConnection(future);
        return future;
    }

    public Future<Void> closeConnection(SQLConnection connection) {
        Future<Void> future = Future.future();
        connection.close(future);
        return future;
    }

    // 开启事务
    public Future<SQLConnection> beginTx() {
        Future<SQLConnection> future = Future.future();
        getConnection().setHandler(r -> {
            if (r.failed()){
                future.fail(r.cause());
                return;
            } else {
                r.result().setAutoCommit(false, tx -> {
                    if (tx.failed()) {
                        future.fail(tx.cause());
                    } else {
                        future.complete(r.result());
                    }
                });
            }
        });
        return future;
    }
    // 回滚
    public Future<Void> rollback(SQLConnection connection) {
        Future<Void> future = Future.future();
        connection.rollback(future);
        return future;
    }

    public Future<List<JsonObject>> queryWithParams(SQLConnection connection, String sql, JsonArray params) {
        Future<List<JsonObject>> future = Future.future();
        connection.queryWithParams(sql, params, r -> {
            connection.close(); // 如果要多查询条件链式操作， 需要去掉 这行代码，自己手动获取connection, 最后手动关闭 connection
            if (r.failed()) {
                logger.error("sql : '"+sql+"'执行异常！"+r.cause().getMessage() + "，参数：" +params.toString());
                future.fail(r.cause());
                return;
            }
            logger.info(" query sql with params : '"+sql+"'正常执行！参数：" +params.toString());
            future.complete(r.result().getRows());
        });
        return future;
    }

    public Future<List<JsonObject>> query(SQLConnection connection, String sql) {
        Future<List<JsonObject>> future = Future.future();
        connection.query(sql, r -> {
            connection.close();
            if (r.failed()) {
                logger.error("sql : '"+sql+"'执行异常！"+r.cause().getMessage());
                future.fail(r.cause());
                return;
            }
            logger.info(" query sql : '"+sql+"'正常执行！");
            future.complete(r.result().getRows());
        });
        return future;
    }

    public Future<Boolean> insertWithParams(SQLConnection connection, String sql, JsonArray params) {
        Future<Boolean> future =  Future.future();
        connection.updateWithParams(sql, params, r -> {
            connection.close();
            if (r.failed()){
                future.fail(r.cause());
                logger.error("sql : '"+sql+"'执行异常！"+r.cause().getMessage()+ "，参数：" +params.toString());
                return;
            }
            logger.info(" insert  sql with params : '"+sql+"'正常执行！参数：" +params.toString());
            future.complete(r.result().getUpdated() > 1 ? true : false);
        });
        return future;
    }


    /**
     * 返回插入的主键
     * @param connection
     * @param sql
     * @param params
     * @return
     */
    public Future<Integer> insertReturnKey(SQLConnection connection, String sql, JsonArray params) {
        Future<Integer> future =  Future.future();
        connection.updateWithParams(sql, params, r -> {
            connection.close();
            if (r.failed()){
                logger.error("sql : '"+sql+"'执行异常！"+r.cause().getMessage()+ "，参数：" +params.toString());
                future.fail(r.cause());
                return;
            }
            logger.info(" insert  sql with params : '"+sql+"'正常执行！参数：" +params.toString());
            future.complete(r.result().getKeys().getInteger(0));
        });
        return future;
    }




    public Future<Boolean> insert(SQLConnection connection, String sql) {
        Future<Boolean> future =  Future.future();
        connection.update(sql, r -> {
            connection.close();
            if (r.failed()){
                logger.error("sql : '"+sql+"'执行异常！"+r.cause().getMessage());
                future.fail(r.cause());
                return;
            }
            logger.info(" insert  sql : '"+sql+"'正常执行！");
            future.complete(r.result().getUpdated() > 1 ? true : false);
        });
        return future;
    }



    public Future<Boolean> updateWithParams(SQLConnection connection, String sql, JsonArray params) {
        return insertWithParams(connection, sql, params);
    }

    public Future<Boolean> update(SQLConnection connection, String sql) {
        return insert(connection, sql);
    }

    public Future<Boolean> deleteWithParams(SQLConnection connection, String sql, JsonArray params) {
        return insertWithParams(connection, sql, params);
    }

    public Future<Boolean> delete(SQLConnection connection, String sql) {
        return insert(connection, sql);
    }


    private String host;

    private int maxPollSize;

    private int port;

    private String username;

    private String password;

    private String database;


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getMaxPollSize() {
        return maxPollSize;
    }

    public void setMaxPollSize(int maxPollSize) {
        this.maxPollSize = maxPollSize;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
