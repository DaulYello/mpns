package com.mpush.mpns.web.handler;

import com.mpush.api.push.MsgType;
import com.mpush.api.push.PushCallback;
import com.mpush.api.push.PushMsg;
import com.mpush.api.push.PushResult;
import com.mpush.api.spi.common.CacheManager;
import com.mpush.api.spi.common.CacheManagerFactory;
import com.mpush.mpns.biz.domain.NotifyDO;
import com.mpush.mpns.biz.service.MPushManager;
import com.mpush.mpns.biz.service.PushService;
import com.mpush.mpns.web.common.ApiResult;
import com.mpush.mpns.web.common.MySqlDaoImpl;
import com.mpush.mpns.web.common.Utils.JdbcUtil;
import com.mpush.tools.Jsons;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;



import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * Created by yxx on 2016/4/26.
 *
 * @author ohun@live.cn
 */
@Controller
public class AdminHandler extends BaseHandler {

    private final Logger logger = LoggerFactory.getLogger("console");

    @Resource
    private PushService pushService;

    @Resource
    private MPushManager mPushManager;

    @Resource
    private MySqlDaoImpl mySqlDao;

    private static CacheManager cacheManager = CacheManagerFactory.create();


    @Override
    protected void initRouter(Router router) {
        router("/admin/push", this::sendPush);
        router("/admin/list/servers", this::listMPushServers);
        router("/admin/get/onlineUserNum", this::getOnlineUserNum);

        router("/client/getRead", this::listMsgToRead);
        router("/client/getPush", this::listMsgToPush);
        router("/client/readMsg", this::readMsg);

        initConsumer(eventBus);
    }

    protected void initConsumer(EventBus eventBus) {
        consumer("/admin/getUser", this::onTestEvent);
    }

    public void listMPushServers(RoutingContext rc) {
        rc.response().end(new ApiResult<>(mPushManager.getConnectServerList()).toString());
    }

    public void getOnlineUserNum(RoutingContext rc) {
        String ip = rc.request().getParam("ip");
        rc.response().end(new ApiResult<>(mPushManager.getOnlineUserNum(ip)).toString());
    }

    /**
     * 后台给前端推送信息
     * @param rc
     */
    public void sendPush(RoutingContext rc) {
        String channel = rc.request().getParam("channel");
        String appkey = rc.request().getParam("appkey");
        String userId = rc.request().getParam("uids");
        String content = rc.request().getParam("content");
        String sender = rc.request().getParam("sender");

        String sql = "select appkey from mp_channel where channel=?";
        if (StringUtils.isBlank(channel) || StringUtils.isBlank(appkey) || StringUtils.isBlank(userId)) {
            logger.info("channel:" + channel + ", blank channel or appkey!");
            rc.response().end(new ApiResult<>(ApiResult.VERTIFY_FAILURE, "none of channel,appkey and uids can be blank!").toString());
            return;
        }

        this.getCached(channel, sql).setHandler(r -> {
            if (!appkey.equals(r.result())){
                logger.debug("channel:"+channel+" appkey don't match passwd");
                rc.response().end(new ApiResult<>(ApiResult.VERTIFY_FAILURE,"wrong appkey!").toString());
                return;
            }
            String insertSql = "insert into uc_notify (content,createAt,sender,type,channel) values (?,?,?,?,?)";
            JsonArray jsonArray = new JsonArray().
                    add(JdbcUtil.getHtmlStringValue(content)).
                    add(JdbcUtil.getLocalDateTime(LocalDateTime.now())).
                    add(JdbcUtil.getStringValue(sender)).
                    add(userId.indexOf(",") > 0 ? 1 : 0).
                    add(JdbcUtil.getStringValue(channel));
            mySqlDao.getConnection()
                    .compose( c -> mySqlDao.insertReturnKey(c,insertSql,jsonArray))
                    .setHandler(res -> {
                        if (res.failed()) {
                            logger.error(res.cause().getMessage());
                            rc.response().end(new ApiResult<>(400, "database error!").toString());
                            return;
                        }

                        PushMsg pushMsg = PushMsg.build(MsgType.NOTIFICATION_AND_MESSAGE, Jsons.toJson(new NotifyDO(content, sender)));
                        pushMsg.setMsgId(String.valueOf(res.result()));

                        boolean success = pushService.notify(userId,pushMsg, new PushCallback() {

                            @Override
                            public void onResult(PushResult result) {
                                String userSql = "insert into uc_user_notify (msgId,uid,sendStatus,resDesc) values (?,?,?,?)";
                                JsonArray userArrary = new JsonArray().add(pushMsg.getMsgId()).
                                        add(JdbcUtil.getStringValue(result.getUserId())).
                                        add(result.getResultCode()).
                                        add(result.getResultDesc());
                                mySqlDao.getConnection()
                                        .compose( conn -> mySqlDao.insertWithParams(conn,userSql,userArrary));
                            }
                        });
                        rc.response().end(new ApiResult<>(success).toString());
                    });
            });

    }


    /**
     * 获取缓存的code,没有则从数据库取
     * @param cacheName
     * @param sql
     * @return
     */
    public Future<String> getCached(String cacheName,String sql) {
        Future<String> future = Future.future();
        String passwd = cacheManager.get(cacheName,String.class);
        if (StringUtils.isNotBlank(passwd)) {
            future.complete(passwd);
        } else {
            mySqlDao.getConnection()
                    .compose(r -> mySqlDao.queryWithParams(r, sql, new JsonArray().add(cacheName)))
                    .setHandler(res -> {
                        if (res.failed()) {
                            logger.error(res.cause().getMessage());
                            future.complete("");
                        }else {
                            String sqlpasswd = res.result() != null && !res.result().isEmpty() ? res.result().get(0).getString("appkey") : "";
                            logger.info("数据库获取appkey成功，结果为："+sqlpasswd);
                            cacheManager.set(cacheName,sqlpasswd,9000);
                            future.complete(sqlpasswd);
                        }
                    });
        }
        return future;
    }

    /**
     * 第一次连接时，获取所有未读信息
     * @param rc
     */
    public void listMsgToRead(RoutingContext rc) {
        String channel = rc.request().getParam("channel");
        String userId = rc.request().getParam("uid");
        if(!StringUtils.isBlank(channel) && !StringUtils.isBlank(userId)) {
            String sql = "select n.msgId,n.content from uc_notify n,uc_user_notify u where n.msgId=u.msgId and n.channel=? and u.uid=? and u.read=0";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(channel)).add(JdbcUtil.getStringValue(userId));
            this.mySqlDao.getConnection().compose((c) -> mySqlDao.queryWithParams(c, sql, params))
                    .setHandler((res) -> {
                        if(res.failed()) {
                            this.logger.error(res.cause().getMessage());
                            rc.response().end((new ApiResult(400, "database error!")).toString());
                        } else {
                            rc.response().end((new ApiResult(res.result().toString())).toString());
                        }
                    });
        } else {
            this.logger.info("uid:" + userId + ", blank channel!");
            rc.response().end((new ApiResult(0, "none of uid and channel can be blank!")).toString());
        }
    }

    /**
     * web端pull，20秒或30秒一次，获取未推送信息
     * @param rc
     */

    public void listMsgToPush(RoutingContext rc) {
        String channel = rc.request().getParam("channel");
        String userId = rc.request().getParam("uid");
        if(!StringUtils.isBlank(channel) && !StringUtils.isBlank(userId)) {
            String sql = "select n.msgId,n.content from uc_notify n,uc_user_notify u where n.msgId=u.msgId and n.channel=? and u.uid=? and u.sendStatus<>1";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(channel)).add(JdbcUtil.getStringValue(userId));
            this.mySqlDao.getConnection().compose((c) -> {
                return this.mySqlDao.queryWithParams(c, sql, params);
            }).setHandler((res) -> {
                if(res.failed()) {
                    this.logger.error(res.cause().getMessage());
                    rc.response().end((new ApiResult(400, "database error!")).toString());
                } else {
                    rc.response().end((new ApiResult(res.result().toString())).toString());
                    String updateSql = "update uc_notify n,uc_user_notify u set u.sendStatus =1 where n.msgId=u.msgId and n.channel=? and u.uid=? and u.sendStatus<>1";
                    this.mySqlDao.getConnection().compose((conn) -> {
                        return this.mySqlDao.updateWithParams(conn, updateSql, params);
                    });
                }
            });
        } else {
            this.logger.info("uid:" + userId + ", blank channel!");
            rc.response().end((new ApiResult(0, "none of uid and channel can be blank!")).toString());
        }
    }

    /**
     * 用户读取某个信息
     * @param rc
     */

    public void readMsg(RoutingContext rc) {
        String msgId = rc.request().getParam("msgId");
        String userId = rc.request().getParam("uid");
        if(!StringUtils.isBlank(msgId) && !StringUtils.isBlank(userId)) {
            String sql = "update uc_user_notify  set  `read` =1 where msgId=? and uid=?";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(msgId)).add(JdbcUtil.getStringValue(userId));
            this.mySqlDao.getConnection().compose((c) -> {
                return this.mySqlDao.updateWithParams(c, sql, params);
            }).setHandler((res) -> {
                if(res.failed()) {
                    this.logger.error(res.cause().getMessage());
                    rc.response().end((new ApiResult(400, "database error!")).toString());
                } else {
                    rc.response().end((new ApiResult("SUCESS")).toString());
                }
            });
        } else {
            this.logger.info("uid:" + userId + ", blank msgId!");
            rc.response().end((new ApiResult(0, "none of uid and msgId can be blank!")).toString());
        }
    }


    public void onTestEvent(Message<JsonObject> event) {
        JsonObject object = event.body();
        object.put("data", "server replay msg");
        event.reply(object);
    }
}
