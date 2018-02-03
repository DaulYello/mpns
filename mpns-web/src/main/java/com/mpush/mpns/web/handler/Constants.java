package com.mpush.mpns.web.handler;

/**
 * Constants class
 */
public final class Constants {

  private Constants() {}

  /** API Route */
  public static final String API_SERVER_PUSH = "/admin/push";
  public static final String API_SERVER_LIST = "/admin/list/servers";
  public static final String API_ONLINENUM_GET = "/admin/get/onlineUserNum";
  public static final String API_UNREADMSG_GET = "/client/toread/:channel/:uid";
  public static final String API_UNPUSHMSG_GET = "/client/topush/:channel/:uid";
  public static final String API_READ_MSG = "/client/read/:uid/:notifyId";


}
