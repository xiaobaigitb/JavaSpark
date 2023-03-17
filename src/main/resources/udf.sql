-- hive on spark
set hive.execution.engine=spark;

-- dynamic partition
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=200000;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.created.files=200000;

--macro for transformation
--  STR_TO_DATE
DROP TEMPORARY MACRO STR_TO_DATE ;
CREATE TEMPORARY MACRO STR_TO_DATE( x STRING ) CAST( FROM_UNIXTIME( UNIX_TIMESTAMP( x, 'yyyyMMdd' ), 'yyyy-MM-dd' ) AS DATE );

-- STR_TO_TIMESTAMP
DROP TEMPORARY MACRO STR_TO_TIMESTAMP ;
CREATE TEMPORARY MACRO STR_TO_TIMESTAMP( x STRING ) CAST( FROM_UNIXTIME( UNIX_TIMESTAMP( rpad(x,14,'0'),'yyyyMMddHHmmss' ), 'yyyy-MM-dd HH:mm:ss' ) AS timestamp );

--  DATE_TO_INT
DROP TEMPORARY MACRO DATE_TO_INT ;
CREATE TEMPORARY MACRO DATE_TO_INT( x DATE ) CAST( DATE_FORMAT( x, 'yyyyMMdd' ) AS INT ) ;

--  TIME_TO_INT
DROP TEMPORARY MACRO TIME_TO_INT ;
CREATE TEMPORARY MACRO TIME_TO_INT( x TIMESTAMP ) CAST( DATE_FORMAT( x, 'yyyyMMdd' ) AS INT) ;

--  TIME_TO_DECIMAL
DROP TEMPORARY MACRO TIME_TO_DECIMAL ;
CREATE TEMPORARY MACRO TIME_TO_DECIMAL( x TIMESTAMP ) CAST( DATE_FORMAT( x, 'yyyyMMdd' ) AS INT) ;

--  DATE_TO_STR
DROP TEMPORARY MACRO DATE_TO_STR ;
CREATE TEMPORARY MACRO DATE_TO_STR( x DATE ) CAST( DATE_FORMAT( x, 'yyyyMMdd' ) AS CHAR(8) ) ;

--  INT_TO_DATE
DROP TEMPORARY MACRO INT_TO_DATE ;
CREATE TEMPORARY MACRO INT_TO_DATE( x INT ) CAST( FROM_UNIXTIME( UNIX_TIMESTAMP( CAST( x AS CHAR(8) ), 'yyyyMMdd' ), 'yyyy-MM-dd' ) AS DATE );

--  GENERATE_CHANNELNO
DROP TEMPORARY MACRO GENERATE_CHANNELNO ;
CREATE TEMPORARY MACRO GENERATE_CHANNELNO( x STRING ) CONCAT ( '00010', x ) ;

--  TANO_TRANS
DROP TEMPORARY MACRO TANO_TRANS ;
CREATE TEMPORARY MACRO TANO_TRANS( x STRING ) IF( x = '98', '99', x ) ;

--  LAST_DAY
DROP TEMPORARY MACRO LAST_DAY ;
CREATE TEMPORARY MACRO LAST_DAY( x DATE ) DATE_SUB( CONCAT_WS( '-', SUBSTR( x, 1, 4 ), CAST( INT( SUBSTR( x, 6, 2 ) ) + 1 AS CHAR(2) ) , '01' ), 1 )  ;
-- SELECT LAST_DAY( CURRENT_DATE ) ;

--  LST_END_OF_YEAR
DROP TEMPORARY MACRO LST_END_OF_YEAR ;
CREATE TEMPORARY MACRO LST_END_OF_YEAR( x DATE, y INT ) DATE_SUB( CONCAT( CAST( YEAR( x ) + y + 1 AS CHAR(4) ) , '-01-01' ), 1 ) ;
-- SELECT LST_END_OF_YEAR( CURRENT_DATE, 1 ) ;
-- SELECT LST_END_OF_YEAR( CURRENT_DATE, 0 ) ;
-- SELECT LST_END_OF_YEAR( CURRENT_DATE, -1 ) ;

DROP TEMPORARY MACRO STT_OF_MTH ;
CREATE TEMPORARY MACRO STT_OF_MTH( x DATE ) CONCAT( SUBSTR( x, 1, 7 ), '-01' ) ;
-- SELECT STT_OF_MTH( CURRENT_DATE ) ;

---将空值替换为空串
DROP TEMPORARY MACRO NullToEmpty ;
CREATE TEMPORARY MACRO NullToEmpty( x STRING ) if(isnull(x),'',x);

-----检查客户姓名是否合法，输入:客户类型、客户姓名
--个人客户：名称中含0~9数字，或含特殊字符（！# $ % & * + = ? ,)，或名称长度<=1
--机构客户：名称中含0~9数字超过5个，或含特殊字符（！# $ % & * + = ? ,)，或名称长度<=1

DROP TEMPORARY MACRO CheckCustnameValid ;
CREATE TEMPORARY MACRO CheckCustnameValid( x STRING ,y STRING) 
if(x='0',if(rlike(y,'[!#$%&*+=?,]') or rlike(y,'[0-9]{5,}') or length(y)<=1,'N','Y'),
if( rlike(y,'[!#$%&*+=?,0123456789]') or length(y)<=1,'N','Y'));
---生成渠道代码

DROP TEMPORARY MACRO generate_channelno ;
CREATE TEMPORARY MACRO generate_channelno( x STRING ) concat('00010',x);

---将空值替换为0
DROP TEMPORARY MACRO NullToZero ;
CREATE TEMPORARY MACRO NullToZero( x decimal(38,10) ) if(isnull(x),0,x);

---input_integer：输入的值，判断是否为空
---replace_integer：为空时，用于替换的值
DROP TEMPORARY MACRO nvlInteger ;
CREATE TEMPORARY MACRO nvlInteger( x int,y int ) if(isnull(x),y,x);
---
DROP TEMPORARY MACRO nvlStr ;
CREATE TEMPORARY MACRO nvlStr( x STRING,y STRING ) if(isnull(x),y,x);