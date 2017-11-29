/**
系统推送的验证码
 */
CREATE TABLE IF NOT EXISTS`mp_channel` (
  `channel` VARCHAR(20) NOT NULL,
  `appkey` VARCHAR(20) NOT NULL,
  PRIMARY KEY (`channel`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;