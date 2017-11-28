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

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource
    private PushService pushService;

    @Resource
    private MPushManager mPushManager;

    @Resource
    private MySqlDaoImpl mySqlDao;

    private static CacheManager cacheManager = CacheManagerFactory.create();

    @Override
    public String getRootPath() {
        return "/admin";
    }

    @Override
    protected void initRouter(Router router) {
        routerBlock("/push", this::sendPush);
        router("/list/servers", this::listMPushServers);
        router("/get/onlineUserNum", this::getOnlineUserNum);

        initConsumer(eventBus);
    }

    protected void initConsumer(EventBus eventBus) {
        consumer("/getUser", this::onTestEvent);
    }

    public void listMPushServers(RoutingContext rc) {
        rc.response().end(new ApiResult<>(mPushManager.getConnectServerList()).toString());
    }

    public void getOnlineUserNum(RoutingContext rc) {
        String ip = rc.request().getParam("ip");
        rc.response().end(new ApiResult<>(mPushManager.getOnlineUserNum(ip)).toString());
    }

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
            String insertSql = "insert into uc_notify (content,createAt,sender,type) values (?,?,?,?)";
            JsonArray jsonArray = new JsonArray().
                    add(JdbcUtil.getHtmlStringValue(content)).
                    add(JdbcUtil.getLocalDateTime(LocalDateTime.now())).
                    add(JdbcUtil.getStringValue(sender)).
                    add(userId.indexOf(",") > 0 ? 1 : 0);
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





    public void onTestEvent(Message<JsonObject> event) {
        JsonObject object = event.body();
        object.put("data", "server replay msg");
        event.reply(object);
    }
}
