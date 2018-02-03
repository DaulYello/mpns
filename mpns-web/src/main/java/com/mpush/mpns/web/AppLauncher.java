package com.mpush.mpns.web;

import com.mpush.bootstrap.ServerLauncher;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;

import static io.vertx.core.impl.launcher.commands.BareCommand.VERTX_OPTIONS_PROP_PREFIX;
import static io.vertx.core.logging.LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME;

/**
 * Created by yxx on 2016/5/5.
 *
 * @author ohun@live.cn
 */
public class AppLauncher extends Launcher {

    private JsonObject config;

    public static void main(String[] args) {
        //启动顺序，先启动mpush,再启动mpns
        ServerLauncher mpushlauncher = new ServerLauncher();
//        launcher.init();
        mpushlauncher.start();

        //启动mpns
        AppLauncher launcher = new AppLauncher();
        launcher.dispatch(args);
    }

    //first
    @Override
    public void afterConfigParsed(JsonObject config) {
        super.afterConfigParsed(config);
        this.config = config;
    }

    //second
    @Override
    public void beforeStartingVertx(VertxOptions options) {
        super.beforeStartingVertx(options);
        System.setProperty(LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
        options.setWarningExceptionTime(
                config.getLong(VERTX_OPTIONS_PROP_PREFIX + "warningExceptionTime", options.getWarningExceptionTime())
        );
    }


    @Override
    public void afterStartingVertx(Vertx vertx) {
        super.afterStartingVertx(vertx);
    }

    @Override
    public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
        super.beforeDeployingVerticle(deploymentOptions);
    }

    @Override
    public void handleDeployFailed(Vertx vertx, String mainVerticle, DeploymentOptions deploymentOptions, Throwable cause) {
        super.handleDeployFailed(vertx, mainVerticle, deploymentOptions, cause);
    }
}
