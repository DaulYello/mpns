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
import com.mpush.mpns.web.common.MySqlUtil;
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

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource
    private PushService pushService;

    @Resource
    private MPushManager mPushManager;

    @Resource
    private MySqlUtil mySqlUtil;

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
        consumer("/getUser", this::onTestEvent);
    }

    public void listMPushServers(RoutingContext rc) {
        rc.response().end(new ApiResult<>(mPushManager.getConnectServerList()).toString());
    }

    public void getOnlineUserNum(RoutingContext rc) {
        rc.response().end(new ApiResult<>(mPushManager.getOnlineUserNum()).toString());
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
        String url = rc.request().getParam("redirectUrl");
        String source = rc.request().getParam("source");
        String groupId = rc.request().getParam("group");
        String roleId = rc.request().getParam("character");

        logger.info("push msg request param,channel:{},appkey:{},userId:{},content:{},sender:{},url:{}," +
                "source:{},groupId:{},roleId:{}",channel,appkey,userId,content,sender,url,source,groupId,roleId);

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
            int msgType =  0;
            if (StringUtils.isNotBlank(groupId)){
                msgType = 1;
            }else if (StringUtils.isNotBlank(roleId)){
                msgType = 2;
            }
            String insertSql = "insert into uc_notify (content,createAt,sender,source,type,groupId,characterId,channel,redirectUrl) values (?,?,?,?,?,?,?,?,?)";
            JsonArray jsonArray = new JsonArray().
                    add(JdbcUtil.getHtmlStringValue(content)).
                    add(JdbcUtil.getLocalDateTime(LocalDateTime.now())).
                    add(JdbcUtil.getStringValue(sender)).
                    add(JdbcUtil.getStringValue(source)).
                    add(msgType).
                    add(JdbcUtil.getStringValue(groupId)).
                    add(JdbcUtil.getStringValue(roleId)).
                    add(JdbcUtil.getStringValue(channel)).
                    add(JdbcUtil.getStringValue(url));
            mySqlUtil.getConnection()
                    .compose( c -> mySqlUtil.insertReturnKey(c,insertSql,jsonArray))
                    .setHandler(res -> {
                        if (res.failed()) {
                            logger.error(res.cause().getMessage());
                            rc.response().end(new ApiResult<>(400, "database error!").toString());
                            return;
                        }

                        PushMsg pushMsg = PushMsg.build(MsgType.NOTIFICATION_AND_MESSAGE, Jsons.toJson(new NotifyDO(content, sender,url)));
                        pushMsg.setMsgId(String.valueOf(res.result()));

                        boolean success = pushService.notify(channel,userId,pushMsg, new PushCallback() {

                            @Override
                            public void onResult(PushResult result) {
                                String userSql = "insert into uc_user_notify (notifyId,uid,sendStatus,resDesc) values (?,?,?,?)";
                                JsonArray userArrary = new JsonArray().add(pushMsg.getMsgId()).
                                        add(JdbcUtil.getStringValue(result.getUserId().split("_")[1])).
                                        add(result.getResultCode()).
                                        add(result.getResultDesc());
                                mySqlUtil.getConnection()
                                        .compose( conn -> mySqlUtil.insertWithParams(conn,userSql,userArrary));
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
            mySqlUtil.getConnection()
                    .compose(r -> mySqlUtil.queryWithParams(r, sql, new JsonArray().add(cacheName)))
                    .setHandler(res -> {
                        if (res.failed()) {
                            logger.error(res.cause().getMessage());
                            future.complete("");
                        }else {
                            String sqlpasswd = res.result() != null && !res.result().isEmpty() ? res.result().get(0).getString("appkey") : "";
                            logger.info("Getting appkey success,the result is："+sqlpasswd);
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
            String sql = "select n.notifyId as msgId,n.content from uc_notify n,uc_user_notify u where n.notifyId=" +
                    "u.notifyId and n.channel=? and u.uid=? and u.read=0";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(channel)).add(JdbcUtil.getStringValue(userId));
            this.mySqlUtil.getConnection().compose((c) -> mySqlUtil.queryWithParams(c, sql, params))
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
            String sql = "select n.notifyId as msgId,n.content from uc_notify n,uc_user_notify u where n.notifyId=" +
                    "u.notifyId and n.channel=? and u.uid=? and u.sendStatus<>1";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(channel)).add(JdbcUtil.getStringValue(userId));
            this.mySqlUtil.getConnection().compose((c) -> {
                return this.mySqlUtil.queryWithParams(c, sql, params);
            }).setHandler((res) -> {
                if(res.failed()) {
                    this.logger.error(res.cause().getMessage());
                    rc.response().end((new ApiResult(400, "database error!")).toString());
                } else {
                    rc.response().end((new ApiResult(res.result().toString())).toString());
                    String updateSql = "update uc_notify n,uc_user_notify u set u.sendStatus =1 where n.notifyId=" +
                            "u.notifyId and n.channel=? and u.uid=? and u.sendStatus<>1";
                    this.mySqlUtil.getConnection().compose((conn) -> {
                        return this.mySqlUtil.updateWithParams(conn, updateSql, params);
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
        String msgId = rc.request().getParam("notifyId");
        String userId = rc.request().getParam("uid");
        if(!StringUtils.isBlank(msgId) && !StringUtils.isBlank(userId)) {
            String sql = "update uc_user_notify  set  `read` =1 where notifyId=? and uid=?";
            JsonArray params = (new JsonArray()).add(JdbcUtil.getStringValue(msgId)).add(JdbcUtil.getStringValue(userId));
            this.mySqlUtil.getConnection().compose((c) -> {
                return this.mySqlUtil.updateWithParams(c, sql, params);
            }).setHandler((res) -> {
                if(res.failed()) {
                    this.logger.error(res.cause().getMessage());
                    rc.response().end((new ApiResult(400, "database error!")).toString());
                } else {
                    rc.response().end((new ApiResult("SUCESS")).toString());
                }
            });
        } else {
            this.logger.info("uid:" + userId + ", blank notifyId!");
            rc.response().end((new ApiResult(0, "none of uid and notifyId can be blank!")).toString());
        }
    }


    public void onTestEvent(Message<JsonObject> event) {
        JsonObject object = event.body();
        object.put("data", "server replay msg");
        event.reply(object);
    }
}
